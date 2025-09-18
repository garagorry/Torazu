# CDP Datalake Backup Script

This script creates a validation-only or full backup for a CDP Datalake. It discovers the environment and backup storage location automatically, triggers the backup, and monitors status with a detailed duration summary.

## Requirements

- CDP CLI configured and authenticated
- `jq` installed
- IAM permissions to describe environment and trigger datalake backups

## File

- `cdp_backup_datalake.sh`

## Usage

```
./cdp_backup_datalake.sh --datalake-name <name> [--backup-name <name>] [--validation-only] [--max-minutes <minutes>] [--interval-seconds <seconds>]
./cdp_backup_datalake.sh -d <name> [-b <name>] [--validation-only] [-m <minutes>] [-i <seconds>]
```

### Arguments

- `--datalake-name, -d`: Datalake name (required)
- `--backup-name, -b`: Custom backup name (default: `<datalake-name>-backup-<timestamp>`)
- `--validation-only`: Validate backup plan without creating actual backup
- `--max-minutes, -m`: Max time to poll for status (default: 10)
- `--interval-seconds, -i`: Interval between checks (default: 5)
- `--help, -h`: Show usage

### Examples

```
# Validation-only with defaults
./cdp_backup_datalake.sh --datalake-name iuprocdp-sbx-01-dl --validation-only

# Full backup with custom name and custom timing (15 minutes max, 10s interval)
./cdp_backup_datalake.sh -d iuprocdp-sbx-01-dl -b my-backup -m 15 -i 10
```

## Behavior

1. Describe the datalake to get the environment CRN
2. Resolve the environment name from the CRN
3. Retrieve the backup location from the environment
4. Trigger validation-only or full backup
5. Poll until completion with per-component and total predicted durations

## Notes

- CDP CLI warnings are suppressed (stderr redirected) to keep output clean
- On failure, the script prints `failureReason` and exits non-zero
- Defaults: 10 minutes max, 5s polling interval
