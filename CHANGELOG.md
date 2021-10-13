# Changelog

## Version 0.9.0

### Features
* Implement Multi-part Download [!28](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/28)

## Version 0.8.0

### Features
* Add Decimal to supported types [!26](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/26)
* Add Date to supported types [!27](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/27)
	
## Version 0.7.2

### Fixes
* Use a consistent read then getting registry entries [!25](https://gitlab.invenia.ca/invenia/Datafeeds/DatafeedsCommon/-/merge_requests/25)

## Version 0.7.1

### Fixes
* Explicitly install boto3-stubs extensions [!24](https://gitlab.invenia.ca/invenia/Datafeeds/DatafeedsCommon/-/merge_requests/24)

## Version 0.7.0

### Features
* Implement DynamoWarehouse.migrate [!20](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/20)

## Version 0.6.2

### Fixes
* Only store important metadata fields alongside S3 objects [!19](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/19)
* Use consistent read for DynamoDB GetItem operations [!19](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/19)


## Version 0.6.1

### Fixes
* Fix an incorrect Exception-check when decoding registry[!18](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/18)

## Version 0.6.0

### Features
* Support storing collection-typed type-maps [!17](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/17)

## Version 0.5.2

### Fixes
* Avoid storing duplicate source files [!16](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/16)

## Version 0.5.1

### Fixes
* Round down datetimes to seconds precision [!15](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/15)


## Version 0.5.0

### Features
* Support None Values in Non-required Metadata [!14](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/14)

## Version 0.4.2

### Fix
* Only update Source Data Table item when necessary[!13](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/13)

## Version 0.4.1

### Fix
* Do not overwrite Query Index if one is specified [!12](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/12)

## Version 0.4.0

### Features
* Add backend stack minimum version requirement check [!11](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/11)

## Version 0.3.0

### Features
* Rename S3Warehouse to DynamoWarehouse [!10](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/10)
* Settings file generator function [!10](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/10)
* S3Warehouse implementations part 2[!9](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/9)

## Version 0.2.0

### Features
* First half of the S3Warehouse implementations [!5](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/5)
* Minor return type updates to the Warehouse Interface [!5](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/5)

## Version 0.1.0

### Features

* Deploy releases to our private PyPI repo [!6](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/6), [!7](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/7)
* Data warehouse interface and supported data types. [!1](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/1), [!2](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/2), [!3](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/3), [!4](https://gitlab.invenia.ca/invenia/Datafeeds/DataWarehouse/-/merge_requests/4)
