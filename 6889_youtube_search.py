#!/usr/bin/python

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser


# Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
# tab of
#   https://cloud.google.com/console
# Please ensure that you have enabled the YouTube Data API for your project.
DEVELOPER_KEY = "AIzaSyDOwLCRuAmni2tAy0uLHOtV4KZp_Ic46gI"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def youtube_search(options):
  youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)

  # Call the search.list method to retrieve results matching the specified
  # query term.
  keyword = raw_input("Please input the keyword: ")
  max_num = int(raw_input("Please input the number of the results you want to search (0-50): "))
  search_response = youtube.search().list(
    q=keyword,
    part="id,snippet",
    maxResults=max_num
  ).execute()

  videos = []
  channels = []
  playlists = []
  videoID = []

  # Add each result to the appropriate list, and then display the lists of
  # matching videos, channels, and playlists.
  for search_result in search_response.get("items", []):
    if search_result["id"]["kind"] == "youtube#video":
      videos.append("%s" % (search_result["snippet"]["title"]))
      videoID.append(search_result["id"]["videoId"])
    elif search_result["id"]["kind"] == "youtube#channel":
      channels.append("%s" % (search_result["snippet"]["title"]))

  print "\nVideos:\n", "\n".join(videos), "\n"
  print "\nVideo IDs:\n", "\n".join(videoID), "\n"
  print "Channels:\n", "\n".join(channels), "\n"

def main():
    args = argparser.parse_args()
    try:
      youtube_search(args)
    except HttpError, e:
      print "An HTTP error %d occurred:\n%s" % (e.resp.status, e.content)


if __name__ == '__main__':
    main()
