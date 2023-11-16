import json
from datetime import date


# Custom JSON Encoder for date objects
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


# Sample data
video_data = [
    {
        "Account": "memlisher.mashups",
        "URL": "https://www.tiktok.com/@memlisher.mashups/video/7299887778005880065",
        "likes": "2.3M",
        "comments": "23.5K",
        "saved": "",
        "Caption": "doyalike freddy fazbear #freddyfazbear",
        "Hashtags": ["#freddyfazbear"],
        "DateColected": datetime.date(2023, 11, 15),
        "DatePosted": "5d ago",
    }
]

# Save to a JSON file
with open("video_data.json", "w") as json_file:
    json.dump(video_data, json_file, indent=2, cls=DateEncoder)
