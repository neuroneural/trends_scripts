#!/bin/bash

ulimit -n 65536

# The script assumes it is /root/file_info/partition_script.sh
#
# This script needs to run every day and collect statistics on how
# storage space on user data directories is being utilized.
# One option is to run it at 5am by setting it up in cron as
#
# 0 5 * * * /root/file_info/partition_script.sh
#
# The scrip depends on gnu parallel, awk, mongodb
# To work, it needs to be executed on arctrdcn018

# Create partition lists
cd /root/file_info
ls -d -1 "/data/users1/"**/ > "partitions.txt"
ls -d -1 "/data/users2/"**/ >> "partitions.txt"
ls -d -1 "/data/users3/"**/ >> "partitions.txt"
ls -d -1 "/data/users4/"**/ >> "partitions.txt"

# double check that everything is mounted
ls -lha /data/users1 > /dev/null &
ls -lha /data/users2 > /dev/null &
ls -lha /data/users3 > /dev/null &
ls -lha /data/users4 > /dev/null &
wait

awk '
  BEGIN{srand()}
  {
    if ($0 ~ /users1/) a1[++n1] = $0
    else if ($0 ~ /users2/) a2[++n2] = $0
    else if ($0 ~ /users3/) a3[++n3] = $0
    else if ($0 ~ /users4/) a4[++n4] = $0
  }
  END{
    max = n1 > n2 ? n1 : n2
    max = max > n3 ? max : n3
    max = max > n4 ? max : n4
    for(i=1; i<=max; i++) {
      if(i<=n1) print a1[i]
      if(i<=n2) print a2[i]
      if(i<=n3) print a3[i]
      if(i<=n4) print a4[i]
    }
  }' "partitions.txt" | /trdapps/linux-x86_64/bin/parallel -j 50 /trdapps/linux-x86_64/bin/dust -d 0 -s -c -b -P -p > storage_users_stats.txt
awk '{print $1, $3}' storage_users_stats.txt | sort -hr > storage_users_stats_sorted_all.txt

# Get the current date and time
current_date=$(date +"%Y-%m-%d")
current_time=$(date +"%H:%M:%S")

# Prepare the JSON file for MongoDB import
output_file="storage_users_stats.json"
> "$output_file"

# Function to process each line
process_line() {
 line="$1"
 store=$(echo "$line" | awk '{print $1}')
 directory=$(echo "$line" | awk '{print $2}')
 users=$(echo "$directory" | awk -F'/' '{print $3}' | sed 's/users//')
 top_directory=$(echo "$directory" | awk -F'/' '{print $4}')

 # Create a JSON object for each line
 jq -n --arg store "$store" --arg users "$users" --arg directory "$top_directory" --arg date "$current_date" --arg time "$current_time" --arg full_path "$directory" \
  '{store: $store, users: $users | tonumber, directory: $directory, date: $date, time: $time, full_path: $full_path}'
}

export -f process_line
export current_date
export current_time

# Process each line in parallel and append to JSON file
cat storage_users_stats_sorted_all.txt | /trdapps/linux-x86_64/bin/parallel -j 50 process_line {} | jq -s . >> "$output_file"

# Insert the data into MongoDB
mongoimport --uri="mongodb://arctrdcn018:27017/clusterstats" --collection users_storage --file "$output_file" --jsonArray --upsertFields users,directory,date

# Move file to /tmp
mv storage_users_stats_sorted_all.txt /tmp/
chmod 666 /tmp/storage_users_stats_sorted_all.txt

# Make a backup of the stats database
BACKUP_DIR="/data/users4/splis/data/segmentation/mongobackup"
BACKUP_FILE="clusterstats_backup.gz"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_FILE}"

# Ensure the backup directory exists
mkdir -p "$BACKUP_DIR"

# Create a compressed archive of the clusterstats database.
# The --archive flag creates a single-file backup.
# The --gzip flag compresses the output archive.
# This will overwrite any existing backup file with the same name.
mongodump --uri="mongodb://arctrdcn018:27017/clusterstats" --archive="$BACKUP_PATH" --gzip

# RESTORE INSTRUCTIONS
# To restore the database from the latest backup, execute the following command from a terminal.
# The `--drop` flag is included to ensure a clean restore by deleting the existing collections
# in the clusterstats database before importing the collections from the backup.
#
# mongorestore --uri="mongodb://arctrdcn018:27017" --archive="/data/users4/splis/data/segmentation/mongobackup/clusterstats_backup.gz" --gzip --drop
