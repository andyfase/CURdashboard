package curconvert

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/xitongsys/parquet-go/ParquetHandler"
	"github.com/xitongsys/parquet-go/Plugin/CSVWriter"
)

// Parquet Struct and functions
type parquetFile struct {
	filePath string
	file     *os.File
}

func (pf *parquetFile) Create(name string) (ParquetHandler.ParquetFile, error) {
	file, err := os.Create(name)
	myFile := new(parquetFile)
	myFile.file = file
	return myFile, err

}
func (pf *parquetFile) Open(name string) (ParquetHandler.ParquetFile, error) {
	var (
		err error
	)
	if name == "" {
		name = pf.filePath
	}

	myFile := new(parquetFile)
	myFile.filePath = name
	myFile.file, err = os.Open(name)
	return myFile, err
}

func (pf *parquetFile) Seek(offset int, pos int) (int64, error) {
	return pf.file.Seek(int64(offset), pos)
}

func (pf *parquetFile) Read(b []byte) (n int, err error) {
	return pf.file.Read(b)
}

func (pf *parquetFile) Write(b []byte) (n int, err error) {
	return pf.file.Write(b)
}

func (pf *parquetFile) Close() {
	pf.file.Close()
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

	tempDir string

	CurColumns     []CSVWriter.MetadataType
	CurFiles       []string
	CurColumnTypes map[string]string
	lowerColumns   bool
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
	cur.lowerColumns = true

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

func (c *CurConvert) initS3Downloader(bucket string, arn string, externalID string) (*s3manager.Downloader, error) {

	// get location of bucket
	resp, err := http.Head("https://" + bucket + ".s3.amazonaws.com")
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(resp.Header.Get("X-Amz-Bucket-Region"))})
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
	resp, err := http.Head("https://" + bucket + ".s3.amazonaws.com")
	if err != nil {
		return nil, err
	}

	// Init Session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(resp.Header.Get("X-Amz-Bucket-Region"))})
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
		return err
	}

	// Unmarshall JSON
	var j map[string]interface{}
	err = json.Unmarshal(buff.Bytes(), &j)
	if err != nil {
		return err
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
		columnName = strings.Replace(columnName, ":", "_", -1)

		// We need to perform duplicate check and type check on lowercase'd column name (as all columns will become lowercase's when written)
		columnNameLower := strings.ToLower(columnName)

		// Skip duplicate columns
		if _, ok := seen[columnNameLower]; ok {
			c.skipCols[i] = true
			continue
		}
		// Check for Type over-ride
		colType, ok := c.CurColumnTypes[columnNameLower]
		if !ok {
			colType = "UTF8"
		}
		// Use columnName not columnNameLower as library requires uppercase fields to populate (which are lower-cased during writing via ph.NameToLower)
		c.CurColumns = append(c.CurColumns, CSVWriter.MetadataType{Type: colType, Name: columnName})
		seen[columnNameLower] = true
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
		return "", err
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
	var f ParquetHandler.ParquetFile
	f = &parquetFile{}
	f, err = f.Create(localParquetFile)
	if err != nil {
		return "", err
	}

	// init Parquet writer
	ph := CSVWriter.NewCSVWriterHandler()
	ph.WriteInit(c.CurColumns, f, 10, 30)

	// read all remaining records of CSV file and write to parquet
	i := 1
	for {
		if i%5000 == 0 {
			ph.Flush()
			i = 1
		}

		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		recParquet := make([]*string, len(rec))
		for i := 0; i < len(rec); i++ {
			_, skip := c.skipCols[i]
			if !skip {
				recParquet[i] = &rec[i]
			}
		}
		ph.WriteString(recParquet)
		i++
	}

	ph.Flush()
	ph.NameToLower()
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
		return err
	}

	return nil
}

//
// ConvertCur - Performs Download, Conversion
func (c *CurConvert) ConvertCur() error {

	if err := c.ParseCur(); err != nil {
		return err
	}

	result := make(chan error)
	i := 0
	for reportKey := range c.CurFiles {
		go func(object string) {
			gzipFile, err := c.DownloadCur(object)
			if err != nil {
				result <- err
				return
			}

			parquetFile, err := c.ParquetCur(gzipFile)
			if err != nil {
				result <- err
				return
			}

			if err := c.UploadCur(parquetFile); err != nil {
				result <- err
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
