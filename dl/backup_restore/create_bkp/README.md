# CDP Datalake Backup Script

This script automates creating a validation-only or full backup for a CDP Datalake. It discovers the environment name and backup storage location automatically from the datalake, then triggers the backup and monitors its status with a detailed duration summary.

## Requirements

- CDP CLI Beta configured and authenticated
- `jq` installed
- Access permissions to describe environment and trigger datalake backups

## Script

- File: `cdp_backup_datalake.sh`

## Usage

```
./cdp_backup_datalake.sh --datalake-name <name> [--backup-name <name>] [--validation-only]
./cdp_backup_datalake.sh -d <name> [-b <name>] [--validation-only]
```

### Arguments

- `--datalake-name, -d`: Datalake name to back up (required)
- `--backup-name, -b`: Optional custom backup name (default: `<datalake-name>-backup-<timestamp>`)
- `--validation-only`: Validate backup plan without creating actual backup
- `--help, -h`: Show usage

### Examples

```
# Validation-only with auto backup name
./cdp_backup_datalake.sh -d jdga-it1-aw-dl --validation-only

# Full backup with custom name
./cdp_backup_datalake.sh --datalake-name jdga-it1-aw-dl --backup-name my-backup
```

## What it does

1. Describes the datalake to get the environment CRN
2. Resolves the environment name from the CRN
3. Retrieves the backup location from the environment config
4. Triggers validation-only or full backup
5. Polls backup status until completion and prints predicted durations by component and total

## Notes

- CDP CLI warnings are suppressed to keep output clean
- On failure, the script shows the `failureReason` and exits non-zero
- Timeout after 5 minutes of polling; adjust `MAX_ATTEMPTS` and sleep as needed
