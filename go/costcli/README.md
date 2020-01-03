# Costcli Documentation

## What is it?

Costcli is a command line tool written in Golang that queries Cost and Usage Report (CUR) data using the AWS Athena API. It uses values from a JSON configuration file to aggregate the results into human readable groups. It allows customers to attribute their spend to business units, teams or projects without needing to tag extensively. Output is comma separated values (CSV).

### Usage

```
$ costcli costbytag -h
NAME:
   costcli costbytag - Perform CUR Conversion

USAGE:
   costcli costbytag [command options] [arguments...]

OPTIONS:
   --startDate value, --sd value        Date in YYYMMDD format (default: "280260201")
   --endDate value, --ed value          Date in YYYMMDD format (default: "280260231")
   --database value, --db value         Athena Database to use (default: "cur")
   --table value, --tb value            Athena Table to use
   --mfaSerial value, --mfa value       Optional MFA Serial or ARN
   --resultsLocation value, --rl value  Athena Results Location override
   --region value, -r value             AWS Region Athena Database and Table exist in (default us-east-1) (default: "us-east-1")
   --roleArn value, --arn value         Optional role ARN to assume when querying Athena
   --externalID value, --extid value    Optional role ARN to assume when querying Athena
   --config value, -c value             JSON tag configuration
   --riusage, --ri                      Process RI Usage and append to results
```

### Example commands
```
# standard output
costcli costbytag --database my_database --table my_table_201902 --roleArn arn:aws:iam::1234567890:role/my-role --resultsLocation s3://my-bucket/my_database/ --config ~/my_config.json > output.csv

# RI Usage included
costcli costbytag --database my_database --table my_table_201902 --roleArn arn:aws:iam::1234567890:role/my-role --resultsLocation s3://my-bucket/my_database/ --config ~/my_config.json --ri > output.csv
```

## JSON Configuration

The JSON configuration file is the mechanism for changing what data is returned by the queries and how the results are grouped. 

### Basic structure
```
{
  "sql": {
    "tagmap": "select \"lineitem/productcode\" as service, **TAGS**, sum(\"lineitem/unblendedcost\") as cost from **DB**.**TABLE** where \"lineitem/lineitemtype\" not in ('RIFee', 'Fee') and \"lineitem/productcode\" != 'OCBPremiumSupport' group by \"lineitem/productcode\", **TAGS**",
    "riusage": "select \"lineitem/productcode\"||'RIFee' as service, **TAGS**, sum(\"lineitem/normalizedusageamount\") as normalized_amount, sum(\"lineitem/usageamount\") as amount  from **DB**.**TABLE** where \"lineitem/lineitemtype\" = 'DiscountedUsage' group by \"lineitem/productcode\"||'RIFee', **TAGS**",
    "ricost": "select \"lineitem/productcode\"||'RIFee' as service,  sum(\"lineitem/unblendedcost\") as cost from **DB**.**TABLE** where \"lineitem/lineitemtype\" = 'RIFee' group by \"lineitem/productcode\"||'RIFee'"
  },
  "tagblacklist": {
    "column_name": ["regex"]
  },
  "tagmap": [
    {
      "tags": ["string"],
      "name": "string",
      "map": [
        {
          "value": "string",
          "match": ["string"],
          "regex": ["regex"]
        }
      ]
    }
  ]
}
```

### sql
An object with one or more SQL query templates. Today Costcli supports `tagmap`, `riusage` and `ricost`. These query templates should be considered hard-coded and not modified unless you are an expert user.

### tagblacklist
An object containing columns with values you want excluded from the output and a list of regular expressions used to match them.

Example:

```
# exclude all EC2 and Route53 resource ids
"tagblacklist":{
  "lineitem/resourceid": ["i-.*", "arn:aws:route53.*"]
}
```

### tagmap

An object with three keys: `tags`, `name`, and `map`. The combination specifies which columns to associate with the name of a business unit, project or other human readable group if it matches a set of regular expressions.

As an example, say we want to group costs by project. We know that some project teams use a naming convention for their AWS resources, some use a tag called "app" and some have their own AWS account, so we could choose resource id, app and accound id as our columns.

We then map "Project Alpha" to a regex that matches "alpha", since they use a naming convention. We map "Analytics Pipeline" to a regex that matches "123456789" since that team has a dedicated account id. And we map "Slapshot: Kitten Edition" to a regex that matches on the app tag "slapshot".
 
#### tags

A list of columns whose values will be used for mapping costs to human readable groups.

#### name

A column heading for the human readable group name.

#### map

An object with three keys: `value`, `match` and `regex`.

##### value

A human readable group name.

##### match _(optional)_

An exact match for column values. 

##### regex _(optional)_

A regular expression match for column values.

#### Example

```
"tagmap": [
    {
      "tags": [
        "lineitem/resourceid", 
        "resourcetags/user_app", 
        "lineitem/usageaccountid"
      ],
      "name": "tags",
      "map": [
        {
          "value": "Slapshot: Kitten Edition",
          "match": ["kittens"],
          "regex": ["slap.*", ".*kitten.*]
        }
      ]
    }
  ]
```




