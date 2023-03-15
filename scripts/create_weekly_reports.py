import boto3
from datetime import datetime, timedelta
import json


def download_combine_weekly_json_files(
    end_date,
    bucket_name="coincommit",
):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    start_date = end_date - timedelta(days=6)

    weekly_data = {}

    for i in range((end_date - start_date).days + 1):
        datestr = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        s3_object = bucket.Object(f"reports/{datestr}/{datestr}.json")
        try:
            file_content = s3_object.get()["Body"].read().decode("utf-8")
            daily_data = json.loads(file_content)
        except:
            print(f"Error downloading or parsing data for {datestr}")
        for token, metadata in daily_data.items():
            if token in weekly_data:
                existing_token_data = weekly_data[token]
                existing_token_data["commit_count"] += metadata["commit_count"]
                existing_token_data["lines_of_code"] += metadata["lines_of_code"]
                existing_token_data["commit_messages"].extend(
                    metadata["commit_messages"]
                )
                existing_token_data["distinct_authors"].extend(
                    metadata["distinct_authors"]
                )
                existing_token_data["commit_urls"].extend(metadata["commit_urls"])
                existing_token_data["changed_methods"].extend(
                    metadata["changed_methods"]
                )

                merged_dict = existing_token_data["file_extensions"].copy()
                for filetype, filecount in metadata["file_extensions"].items():
                    if filetype in merged_dict:
                        merged_dict[filetype] += filecount
                    else:
                        merged_dict[filetype] = filecount
                existing_token_data["file_extensions"] = merged_dict

                merged_dict = existing_token_data["loc_changes_by_filetype"].copy()
                for action in ["insertions", "deletions", "net"]:
                    actiondict = merged_dict[action]
                    actiondict2 = metadata["loc_changes_by_filetype"][action]
                    for filetype, filecount in actiondict2.items():
                        if filetype in actiondict:
                            actiondict[filetype] += filecount
                        else:
                            actiondict[filetype] = filecount
                    merged_dict[action] = actiondict
                weekly_data[token]["loc_changes_by_filetype"] = merged_dict
            else:
                weekly_data[token] = metadata

        # de-dupe lists
        for token, metadata in weekly_data.items():
            weekly_data[token]["commit_messages"] = list(
                set(metadata["commit_messages"])
            )
            weekly_data[token]["distinct_authors"] = list(
                set(metadata["distinct_authors"])
            )
            weekly_data[token]["commit_urls"] = list(set(metadata["commit_urls"]))
            weekly_data[token]["changed_methods"] = list(
                set(metadata["changed_methods"])
            )
    weekly_file_content = json.dumps(weekly_data, indent=2)
    weekly_aggregation_path = f"/tmp/weekly.json"
    with open(weekly_aggregation_path, "w") as f:
        f.write(weekly_file_content)

    # s3_location = f'reports/{end_date.strftime("%Y-%m-%d")}/weekly.json'
    # s3.Object(bucket_name, s3_location).put(Body=weekly_file_content)

    return weekly_aggregation_path


if __name__ == "__main__":
    end_date = datetime.today()
    weekly_file_name = download_combine_weekly_json_files(end_date)

    print(f"Weekly data has been saved to {weekly_file_name}")
