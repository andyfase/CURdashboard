package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/urfave/cli"
)

type Map struct {
	Value string   `json:"value"`
	Match []string `json:"match"`
	Regex []string `json:"regex"`
}

type TagMap struct {
	Tags []string `json:"tags"`
	Map  []Map    `json:"map"`
	Name string   `json:"name"`
}

type Config struct {
	TagMap       []TagMap            `json:"tagmap"`
	TagBlacklist map[string][]string `json:"tagblacklist`
	Tags         string
	Database     string
	Table        string
	Account      string
}

type AthenaResponse struct {
	Rows []map[string]string
}

type Results struct {
	tagCosts map[string]float64
	total    float64
}

/*
Function reads in configuration file provided in configFile input
Config file is stored in TOML format
*/
func getConfig(conf *Config, configFile string) error {

	// check for existance of file
	if _, err := os.Stat(configFile); err != nil {
		return errors.New("Config File " + configFile + " does not exist")
	}

	// read file
	b, err := ioutil.ReadFile(configFile)
	if err != nil {
		return errors.New("Error Reading TOML config file: " + err.Error())
	}

	// parse TOML config file into struct
	if err := json.Unmarshal(b, &conf); err != nil {
		return errors.New("Error Decoding config file: " + err.Error())
	}

	return nil
}

func getCreds(arn string, externalID string, mfa string, sess *session.Session) *credentials.Credentials {
	if len(arn) < 1 {
		return nil
	}
	if len(mfa) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.SerialNumber = aws.String(mfa)
			p.TokenProvider = stscreds.StdinTokenProvider
			if len(externalID) > 0 {
				p.ExternalID = aws.String(externalID)
			}
		})
	}
	if len(externalID) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = aws.String(externalID)
		})
	}
	return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {})
}

/*
Function takes SQL to send to Athena converts into JSON to send to Athena HTTP proxy and then sends it.
Then recieves responses in JSON which is converted back into a struct and returned
*/
func sendQuery(svc *athena.Athena, db string, sql string, account string, region string, s3ResultsLocation string) (AthenaResponse, error) {

	var results AthenaResponse
	var s athena.StartQueryExecutionInput
	s.SetQueryString(sql)

	var q athena.QueryExecutionContext
	q.SetDatabase(db)
	s.SetQueryExecutionContext(&q)

	var r athena.ResultConfiguration
	if len(s3ResultsLocation) > 1 {
		r.SetOutputLocation(s3ResultsLocation)
	} else {
		r.SetOutputLocation("s3://aws-athena-query-results-" + account + "-" + region + "/")
	}
	s.SetResultConfiguration(&r)

	result, err := svc.StartQueryExecution(&s)
	if err != nil {
		return results, errors.New("Error Querying Athena, StartQueryExecution: " + err.Error())
	}

	var qri athena.GetQueryExecutionInput
	qri.SetQueryExecutionId(*result.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second // Pause for 2 seconds

	for {
		qrop, err = svc.GetQueryExecution(&qri)
		if err != nil {
			return results, errors.New("Error Querying Athena, GetQueryExecution: " + err.Error())
		}
		if *qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}
		time.Sleep(duration)
	}

	if *qrop.QueryExecution.Status.State != "SUCCEEDED" {
		return results, errors.New("Error Querying Athena, completion state is NOT SUCCEEDED, state is: " + *qrop.QueryExecution.Status.State)
	}

	var ip athena.GetQueryResultsInput
	ip.SetQueryExecutionId(*result.QueryExecutionId)

	// loop through results (paginated call)
	var colNames []string
	err = svc.GetQueryResultsPages(&ip,
		func(page *athena.GetQueryResultsOutput, lastPage bool) bool {
			for row := range page.ResultSet.Rows {
				if len(colNames) < 1 { // first row contains column names - which we use in any subsequent rows to produce map[columnname]values
					for j := range page.ResultSet.Rows[row].Data {
						colNames = append(colNames, *page.ResultSet.Rows[row].Data[j].VarCharValue)
					}
				} else {
					result := make(map[string]string)
					skip := false
					for j := range page.ResultSet.Rows[row].Data {
						if j < len(colNames) {
							if page.ResultSet.Rows[row].Data[j].VarCharValue == nil {
								skip = true
								break
							}
							result[colNames[j]] = *page.ResultSet.Rows[row].Data[j].VarCharValue
						}
					}
					if len(result) > 0 && !skip {
						results.Rows = append(results.Rows, result)
					}
				}
			}
			if lastPage {
				return false // return false to end paginated calls
			}
			return true // keep going if there are more pages to fetch
		})
	if err != nil {
		return results, errors.New("Error Querying Athena, GetQueryResultsPages: " + err.Error())
	}

	return results, nil
}

func buildCostQuery(c *Config) string {
	for _, tm := range c.TagMap {
		for i := range tm.Tags {
			c.Tags += "\"" + tm.Tags[i] + "\","
		}
	}
	c.Tags = c.Tags[:len(c.Tags)-1]

	sql := "select \"lineitem/productcode\" as service," +
		c.Tags +
		", sum(\"lineitem/blendedcost\") as cost from " +
		c.Database + "." + c.Table +
		" where \"lineitem/lineitemtype\" not in ('RIFee', 'Tax')" +
		" group by \"lineitem/productcode\", " + c.Tags
	return sql
}

func findExact(value string, list []string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}

func findRegex(value string, list []string) bool {
	for _, v := range list {
		r, err := regexp.Compile(v)
		if err != nil {
			fmt.Println("Regex: " + v + ", invalid - skipping")
			continue
		}
		if r.MatchString(value) {
			return true
		}
	}
	return false
}

func findTagMatch(match string, m []Map, tag string, blacklist map[string][]string) (string, error) {
	for _, object := range m {
		if findExact(match, object.Match) {
			return object.Value, nil
		}
	}

	for _, object := range m {
		if findRegex(match, object.Regex) {
			return object.Value, nil
		}
	}

	tagblacklist, ok := blacklist[tag]
	if ok {
		if findRegex(match, tagblacklist) {
			return "", fmt.Errorf("No Match")
		}
	}
	if len(match) > 0 {
		return match, nil
	}

	return "", fmt.Errorf("No Match")
}

func processResults(resp AthenaResponse, c Config) Results {

	r := &Results{
		tagCosts: make(map[string]float64),
		total:    0,
	}

	for _, row := range resp.Rows {
		f, err := strconv.ParseFloat(row["cost"], 64)
		if err != nil {
			fmt.Println("Failed to convert float, continuing")
			continue
		}

		tags := []string{row["service"]}
		for _, tm := range c.TagMap {
			found := false
			for i := range tm.Tags {
				match, err := findTagMatch(row[tm.Tags[i]], tm.Map, tm.Tags[i], c.TagBlacklist)
				if err == nil {
					tags = append(tags, match)
					found = true
					break
				}
			}
			if !found {
				tags = append(tags, "Untagged")
			}
		}
		r.tagCosts[strings.Join(tags, ",")] += f
		r.total += f
	}
	return *r
}

func printResults(r Results, c Config) {

	var keys []string
	for k := range r.tagCosts {
		keys = append(keys, k)
	}

	var tagNames string
	for _, v := range c.TagMap {
		tagNames += "\"" + v.Name + "\","
	}

	sort.Strings(keys)

	fmt.Println("\"service\"," + tagNames + "\"amount\"")
	for _, k := range keys {
		if math.Round(r.tagCosts[k]/0.01)*0.01 > 0.01 {
			fmt.Printf("%s,%.2f\n", k, math.Round(r.tagCosts[k]/0.01)*0.01)
		}
	}
	fmt.Println("---------------------")
	fmt.Printf("Total: %.2f", math.Round(r.total/0.01)*0.01)
}

func main() {
	app := cli.NewApp()
	app.Name = "Cost CLI"
	app.Usage = "Command Line Interface for download, conversion and re-upload of the AWS CUR from/to a S3 Bucket."
	app.Version = "1.0.0"

	var startDate, endDate, database, table, region, roleArn, externalID, configFile, s3ResultsLocation, mfa string
	app.Commands = []cli.Command{
		{
			Name:  "costbytag",
			Usage: "Perform CUR Conversion",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "startDate, sd",
					Usage:       "Date in YYYMMDD format",
					Value:       time.Now().Format("201601") + "01",
					Destination: &startDate,
				},
				cli.StringFlag{
					Name:        "endDate, ed",
					Usage:       "Date in YYYMMDD format",
					Value:       time.Now().Format("201601") + "31",
					Destination: &endDate,
				},
				cli.StringFlag{
					Name:        "database, db",
					Usage:       "Athena Database to use",
					Value:       "cur",
					Destination: &database,
				},
				cli.StringFlag{
					Name:        "table, tb",
					Usage:       "Athena Table to use",
					Value:       "",
					Destination: &table,
				},
				cli.StringFlag{
					Name:        "mfaSerial, mfa",
					Usage:       "Optional MFA Serial or ARN",
					Value:       "",
					Destination: &mfa,
				},
				cli.StringFlag{
					Name:        "resultsLocation, rl",
					Usage:       "Athena Results Location override",
					Value:       "",
					Destination: &s3ResultsLocation,
				},
				cli.StringFlag{
					Name:        "region, r",
					Usage:       "AWS Region Athena Database and Table exist in (default us-east-1)",
					Value:       "us-east-1",
					Destination: &region,
				},
				cli.StringFlag{
					Name:        "roleArn, arn",
					Usage:       "Optional role ARN to assume when querying Athena",
					Value:       "",
					Destination: &roleArn,
				},
				cli.StringFlag{
					Name:        "externalID, extid",
					Usage:       "Optional role ARN to assume when querying Athena",
					Value:       "",
					Destination: &externalID,
				},
				cli.StringFlag{
					Name:        "config, c",
					Usage:       "JSON tag configuration",
					Value:       "",
					Destination: &configFile,
				},
			},
			Action: func(c *cli.Context) error {

				if len(table) < 1 {
					cli.ShowCommandHelp(c, "costbytag")
					log.Fatalln("Must supply a Athena Table to query")
				}

				// read in config file
				var conf Config
				if err := getConfig(&conf, configFile); err != nil {
					return err
				}
				conf.Database = database
				conf.Table = table

				sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
				if err != nil {
					return err
				}

				// if needed set creds for AssumeRole and reset session
				if len(roleArn) > 0 {
					sess = sess.Copy(&aws.Config{Credentials: getCreds(roleArn, externalID, mfa, sess)})
				}

				// fetch account ID
				svc := sts.New(sess)
				result, err := svc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
				if err != nil {
					return err
				}
				conf.Account = *result.Account

				// Cost Breakdown
				svcAthena := athena.New(sess)
				response, err := sendQuery(svcAthena, conf.Database, buildCostQuery(&conf), conf.Account, region, s3ResultsLocation)
				if err != nil {
					return err
				}
				printResults(processResults(response, conf), conf)

				return nil
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
	}
}
