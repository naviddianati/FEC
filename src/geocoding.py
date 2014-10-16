from requests_oauthlib import OAuth1
import requests
import json
import time



 # Replace the following variables with your personal ones
key = 'AIzaSyA3Ha2sWLO2VT0QRwEoY0CG5uj5sw1UXWo'
url = 'https://maps.googleapis.com/maps/api/geocode/json'


def get_coords(address):
    params = {'key': key,
          'address' :address}

    resp = requests.get(url = url,params = params)
    r = resp.json()
    return r['results'][0]['geometry']['location']







