import boto3
import datetime
import json


def download_combine_weekly_json_files(bucket_name, start_date_str):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = start_date + datetime.timedelta(days=6)

    weekly_data = {}

    for i in range((end_date - start_date).days + 1):
        date = (start_date + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        s3_object = bucket.Object(date)
        try:
            file_content = s3_object.get()["Body"].read().decode("utf-8")
            daily_data = json.loads(file_content)
            for key, value in daily_data.items():
                if key in weekly_data:
                    weekly_data[key] += value
                else:
                    weekly_data[key] = value
        except:
            print(f"Error downloading or parsing data for {date}")

    weekly_file_name = (
        f'{start_date_str}_to_{end_date.strftime("%Y-%m-%d")}_weekly.json'
    )
    weekly_file_content = json.dumps(weekly_data)
    s3.Object(bucket_name, weekly_file_name).put(Body=weekly_file_content)

    return weekly_file_name


if __name__ == "__main__":
    bucket_name = "coincommit/reports"
    start_date_str = "2023-01-01"

    weekly_file_name = download_combine_weekly_json_files(bucket_name, start_date_str)

    print(f"Weekly data has been saved to {weekly_file_name}")
