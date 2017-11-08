package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/andyfase/CURDashboard/go/curconvert"
	"github.com/urfave/cli"
)

func main() {

	app := cli.NewApp()
	app.Name = "CURConvertCLI"
	app.Usage = "Command Line Interface for download, conversion and re-upload of the AWS CUR from/to a S3 Bucket."
	app.Version = "1.0.0"

	var sourceBucket, destBucket, destPath, reportPath, reportName, inputDate, sourceRoleArn, sourceExternalID, destRoleArn, destExternalID string
	app.Commands = []cli.Command{
		{
			Name:  "convert",
			Usage: "Perform CUR Conversion",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "sourceBucket, sb",
					Usage:       "Source Bucket which contains the CUR",
					Destination: &sourceBucket,
				},
				cli.StringFlag{
					Name:        "destBucket, db",
					Usage:       "Destination Bucket. (Optional) define if not the same as source",
					Destination: &destBucket,
				},
				cli.StringFlag{
					Name:        "destPath, dp",
					Usage:       "Destination Path to store converted CUR. (Optional) defaults to parquet-cur/YYYYMM/",
					Value:       "",
					Destination: &destPath,
				},
				cli.StringFlag{
					Name:        "reportPath, rp",
					Usage:       "CUR Report Path - defined when creating the AWS report",
					Value:       "",
					Destination: &reportPath,
				},
				cli.StringFlag{
					Name:        "reportName, rn",
					Usage:       "CUR Report Name - defined when creating the AWS report",
					Destination: &reportName,
				},
				cli.StringFlag{
					Name:        "month, m",
					Usage:       "Month of CUR to convert. (Optional) do not define for current CUR. Format YYYYMM",
					Value:       "",
					Destination: &inputDate,
				},
				cli.StringFlag{
					Name:        "sourceRole, sr",
					Usage:       "Source Role ARN to assume when downloading CUR. (Optional) define if required to assume cross account role for download",
					Value:       "",
					Destination: &sourceRoleArn,
				},
				cli.StringFlag{
					Name:        "sourceExternalID, sextid",
					Usage:       "Source External ID used when assuming source role. (Optional) ",
					Value:       "",
					Destination: &sourceExternalID,
				},
				cli.StringFlag{
					Name:        "destRole, dr",
					Usage:       "Destination Role ARN to assume when uploading CUR. (Optional) define if required to assume cross account role for upload",
					Value:       "",
					Destination: &destRoleArn,
				},
				cli.StringFlag{
					Name:        "destExternalID, dextid",
					Usage:       "Source External ID used when assuming destination role. (Optional) ",
					Value:       "",
					Destination: &destExternalID,
				},
			},
			Action: func(c *cli.Context) error {

				if len(sourceBucket) < 1 {
					cli.ShowCommandHelp(c, "convert")
					log.Fatalln("Must supply a source bucket")

				}

				if len(destBucket) < 1 {
					destBucket = sourceBucket
				}

				var start time.Time
				if len(inputDate) < 6 {
					start = time.Now()
				} else {
					start, _ = time.Parse("200601", inputDate)
				}

				// Generate CUR Date Format which is YYYYMM01-YYYYMM01
				end := start.AddDate(0, 1, 0)
				curDate := start.Format("200601") + "01-" + end.Format("200601") + "01"

				// Set defined format for CUR manifest
				manifest := reportPath + "/" + curDate + "/" + reportName + "-Manifest.json"

				// Set or extend destPath
				if len(destPath) < 1 {
					destPath = "parquet-cur/" + start.Format("200601")
				} else {
					destPath += "/" + start.Format("200601")
				}

				// Init CUR Converter
				cc := curconvert.NewCurConvert(sourceBucket, manifest, destBucket, destPath)

				// Set Source Role if required
				if len(sourceRoleArn) > 1 {
					cc.SetSourceRole(sourceRoleArn, sourceExternalID)
				}

				// Set Destination Role if required
				if len(destRoleArn) > 1 {
					cc.SetDestRole(destRoleArn, destExternalID)
				}

				// Convert CUR
				if err := cc.ConvertCur(); err != nil {
					log.Fatalln(err)
				}

				fmt.Println("CUR conversion completed and available at s3://" + destBucket + "/" + destPath + "/")
				return nil
			},
		},
	}

	app.Run(os.Args)
}
