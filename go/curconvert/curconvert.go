package curconvert

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/Plugin/CSVWriter"
)

//
type CurColumn struct {
	Name string
	Type string
}

//
// CurConvert class and functions
type CurConvert struct {
	sourceBucket string
	sourceObject string
	destBucket   string
	destObject   string

	sourceArn        string
	sourceExternalID string
	destArn          string
	destExternalID   string

	tempDir     string
	concurrency int

	CurColumns     []CSVWriter.MetadataType
	CurFiles       []string
	CurColumnTypes map[string]string
	skipCols       map[int]bool
}

//
// NewCurConvert - Init struct
func NewCurConvert(sBucket string, sObject string, dBucket string, dObject string) *CurConvert {
	cur := new(CurConvert)
	cur.sourceBucket = sBucket
	cur.sourceObject = sObject
	cur.destBucket = dBucket
	cur.destObject = dObject

	cur.tempDir = "/tmp"
	cur.concurrency = 10

	// over-ride CUR column types
	cur.CurColumnTypes = make(map[string]string)
	cur.CurColumnTypes["lineitem/usageamount"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/normalizationfactor"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/normalizedusageamount"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/unblendedrate"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/unblendedcost"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/blendedrate"] = "DOUBLE"
	cur.CurColumnTypes["lineitem/blendedcost"] = "DOUBLE"
	cur.CurColumnTypes["pricing/publicondemandcost"] = "DOUBLE"
	cur.CurColumnTypes["pricing/publicondemandrate"] = "DOUBLE"
	cur.CurColumnTypes["reservation/normalizedunitsperreservation"] = "DOUBLE"
	cur.CurColumnTypes["reservation/totalreservednormalizedunits"] = "DOUBLE"
	cur.CurColumnTypes["reservation/totalreservedunits"] = "DOUBLE"
	cur.CurColumnTypes["reservation/unitsperreservation"] = "DOUBLE"

	return cur
}

//
// SetSourceRole - configures the source profile data for retrieving CUR from different AWS account
func (c *CurConvert) SetSourceRole(arn string, externalID string) error {
	if len(arn) < 1 {
		return errors.New("Must supply a Role ARN")
	}
	c.sourceArn = arn
	c.sourceExternalID = externalID
	return nil
}

//
// SetDestRole - configures the source profile data for retrieving CUR from different AWS account
func (c *CurConvert) SetDestRole(arn string, externalID string) error {
	if len(arn) < 1 {
		return errors.New("Must supply a Role ARN")
	}
	c.destArn = arn
	c.destExternalID = externalID
	return nil
}

//
// SetCURManifest - configures the source manifest object for retrieving CUR from different AWS account
func (c *CurConvert) SetSourceManifest(manifest string) error {
	if len(manifest) < 1 {
		return errors.New("Must supply a Manifest")
	}
	c.sourceObject = manifest
	return nil
}

//
// SetDestPath - configures the dest pat for the converted CUR
func (c *CurConvert) SetDestPath(path string) error {
	if len(path) < 1 {
		return errors.New("Must supply a Path")
	}
	c.destObject = path
	return nil
}

//
// GetCURColumns - Converts processed CUR columns into map and returns it
func (c *CurConvert) GetCURColumns() ([]CurColumn, error) {

	if c.CurColumns == nil || len(c.CurColumns) < 1 {
		return nil, errors.New("cannot fetch CUR column data, call ParseCUR first")
	}

	cols := []CurColumn{}
	for i := 0; i < len(c.CurColumns); i++ {
		if c.CurColumns[i].Type == "UTF8" {
			c.CurColumns[i].Type = "STRING"
		}
		cols = append(cols, CurColumn{Name: c.CurColumns[i].Name, Type: c.CurColumns[i].Type})
	}

	return cols, nil
}

func (c *CurConvert) getCreds(arn string, externalID string, sess *session.Session) *credentials.Credentials {
	if len(arn) < 1 {
		return nil
	}
	if len(externalID) > 0 {
		return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {
			p.ExternalID = &externalID
		})
	}
	return stscreds.NewCredentials(sess, arn, func(p *stscreds.AssumeRoleProvider) {})
}

func (c *CurConvert) getBucketLocation(bucket string, arn string, externalID string) (string, error) {

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1")})
	if err != nil {
		return "", err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	// Get Bucket location
	svc := s3.New(sess)
	res, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return "", err
	}

	// empty string returned for buckets existing in us-east-1! https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
	if res.LocationConstraint == nil || len(*res.LocationConstraint) < 1 {
		return "us-east-1", nil
	}
	return *res.LocationConstraint, nil
}

func (c *CurConvert) initS3Downloader(bucket string, arn string, externalID string) (*s3manager.Downloader, error) {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(bucket, arn, externalID)
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(bucketLocation), DisableRestProtocolURICleaning: aws.Bool(true)})
	if err != nil {
		return nil, err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	return s3manager.NewDownloader(sess), nil
}

func (c *CurConvert) initS3Uploader(bucket string, arn string, externalID string) (*s3manager.Uploader, error) {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(bucket, arn, externalID)
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(bucketLocation), DisableRestProtocolURICleaning: aws.Bool(true)})
	if err != nil {
		return nil, err
	}

	// if needed set creds for AssumeRole and reset session
	if len(arn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(arn, externalID, sess)})
	}

	return s3manager.NewUploader(sess), nil
}

//
// CheckCURExists - Attempts to fetch manifest file to confirm existence of CUR
func (c *CurConvert) CheckCURExists() error {

	// get location of bucket
	bucketLocation, err := c.getBucketLocation(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(bucketLocation), DisableRestProtocolURICleaning: aws.Bool(true)})
	if err != nil {
		return err
	}

	// if needed set creds for AssumeRole and reset session
	if len(c.sourceArn) > 0 {
		sess = sess.Copy(&aws.Config{Credentials: c.getCreds(c.sourceArn, c.sourceExternalID, sess)})
	}

	svc := s3.New(sess)
	_, err = svc.GetObject(
		&s3.GetObjectInput{
			Bucket: aws.String(c.sourceBucket),
			Key:    aws.String(c.sourceObject),
		})

	return err
}

//
// ParseCur - Reads JSON manifest file from S3 and adds needed data into struct
func (c *CurConvert) ParseCur() error {

	// init S3 manager
	s3dl, err := c.initS3Downloader(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return err
	}

	// Download CUR manifest JSON
	buff := &aws.WriteAtBuffer{}
	_, err = s3dl.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(c.sourceBucket),
		Key:    aws.String(c.sourceObject),
	})
	if err != nil {
		return fmt.Errorf("failed to download manifest, bucket: %s, object: %s, error: %s", c.sourceBucket, c.sourceObject, err.Error())
	}

	// Unmarshall JSON
	var j map[string]interface{}
	err = json.Unmarshal(buff.Bytes(), &j)
	if err != nil {
		return fmt.Errorf("failed to parse manifest, bucket: %s, object: %s, error: %s", c.sourceBucket, c.sourceObject, err.Error())
	}

	// Store all column names from manifests
	cols := j["columns"].([]interface{})
	seen := make(map[string]bool)
	c.skipCols = make(map[int]bool)
	i := -1
	for column := range cols {
		i++
		t := cols[column].(map[string]interface{})
		columnName := t["category"].(string) + "/" + t["name"].(string)

		// convert columns names to allowed characters (lowercase) and substitute '_' for any non-allowed character
		columnName = strings.ToLower(columnName)
		r := func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z':
				return r
			case r >= '0' && r <= '9':
				return r
			case r == '/':
				return r
			default:
				return '_'
			}
		}
		columnName = strings.Map(r, columnName)

		// Skip duplicate columns
		if _, ok := seen[columnName]; ok {
			c.skipCols[i] = true
			continue
		}
		// Check for type over-ride
		colType, ok := c.CurColumnTypes[columnName]
		if !ok {
			colType = "UTF8"
		}

		c.CurColumns = append(c.CurColumns, CSVWriter.MetadataType{Type: colType, Name: columnName})
		seen[columnName] = true
	}

	// Store CSV CUR files
	reportKeys := j["reportKeys"].([]interface{})
	for key := range reportKeys {
		c.CurFiles = append(c.CurFiles, reportKeys[key].(string))
	}
	return nil
}

//
// DownloadCur -
func (c *CurConvert) DownloadCur(curObject string) (string, error) {

	// define localfile name
	localFile := c.tempDir + "/" + curObject[strings.LastIndex(curObject, "/")+1:]

	// create localfile
	file, err := os.Create(localFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// init S3 manager
	s3dl, err := c.initS3Downloader(c.sourceBucket, c.sourceArn, c.sourceExternalID)
	if err != nil {
		return "", err
	}

	// download S3 object to file
	_, err = s3dl.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(c.sourceBucket),
			Key:    aws.String(curObject),
		})

	if err != nil {
		return "", fmt.Errorf("failed to download CUR object, bucket: %s, object: %s, error: %s", c.sourceBucket, curObject, err.Error())
	}

	return localFile, nil
}

//
// ParquetCur -
func (c *CurConvert) ParquetCur(inputFile string) (string, error) {

	// open input
	file, err := os.Open(inputFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// init gzip library on input
	gr, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	// init csv reader
	cr := csv.NewReader(gr)

	// read and ignore header record
	_, err = cr.Read()
	if err != nil {
		log.Fatal(err)
	}

	// create local parquet file
	localParquetFile := c.tempDir + "/" + inputFile[strings.LastIndex(inputFile, "/")+1:strings.Index(inputFile, ".")] + ".parquet"
	f, err := ParquetFile.NewLocalFileWriter(localParquetFile)
	if err != nil {
		return "", fmt.Errorf("failed to create parquet file %s, error: %s", localParquetFile, err.Error())
	}

	// init Parquet writer
	ph, err := CSVWriter.NewCSVWriter(c.CurColumns, f, int64(c.concurrency))
	if err != nil {
		return "", err
	}

	// read all remaining records of CSV file and write to parquet
	i := 1
	for {
		if i%5000 == 0 {
			ph.Flush(true)
			i = 1
		}

		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		var recParquet []*string
		for k, _ := range rec {
			_, skip := c.skipCols[k]
			if !skip {
				recParquet = append(recParquet, &rec[k])
			}
		}
		ph.WriteString(recParquet)
		i++
	}

	ph.Flush(true)
	ph.WriteStop()
	f.Close()

	return localParquetFile, nil
}

//
// UploadCur -
func (c *CurConvert) UploadCur(parquetFile string) error {

	uploadFile := c.destObject + "/" + parquetFile[strings.LastIndex(parquetFile, "/")+1:]

	// init S3 manager
	s3up, err := c.initS3Uploader(c.destBucket, c.destArn, c.destExternalID)
	if err != nil {
		return err
	}

	// open file
	file, err := os.Open(parquetFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Upload CUR manifest JSON
	_, err = s3up.Upload(&s3manager.UploadInput{
		Bucket: aws.String(c.destBucket),
		Key:    aws.String(uploadFile),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("failed to upload CUR parquet object, bucket: %s, object: %s, error: %s", c.destBucket, uploadFile, err.Error())
	}

	return nil
}

//
// ConvertCur - Performs Download, Conversion
func (c *CurConvert) ConvertCur() error {

	if err := c.ParseCur(); err != nil {
		return fmt.Errorf("Error Parsing CUR Manifest: %s", err.Error())
	}

	result := make(chan error)
	i := 0
	for reportKey := range c.CurFiles {
		go func(object string) {
			gzipFile, err := c.DownloadCur(object)
			if err != nil {
				result <- fmt.Errorf("Error Downloading CUR: %s", err.Error())
				return
			}

			parquetFile, err := c.ParquetCur(gzipFile)
			if err != nil {
				result <- fmt.Errorf("Error Converting CUR: %s", err.Error())
				return
			}

			if err := c.UploadCur(parquetFile); err != nil {
				result <- fmt.Errorf("Error Uploading CUR: %s", err.Error())
				return
			}

			os.Remove(gzipFile)
			os.Remove(parquetFile)
			result <- nil
		}(c.CurFiles[reportKey])
		i++
	}

	// wait for jobs to complete
	for w := 0; w < i; w++ {
		err := <-result
		if err != nil {
			return err
		}
	}

	return nil
}
