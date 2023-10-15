import json
import requests

customer_journeys = [{'conversion_id': 'EU02785522',
  'session_id': '2020-12-27_0001_DE_82a8917d-d9cb-a665-a1ce-34e9a279445c',
  'timestamp': '2020-12-27 18:04:24',
  'channel_label': 'Shopping - Brand',
  'holder_engagement': 0,
  'closer_engagement': 0,
  'conversion': 0,
  'impression_interaction': 0},
 {'conversion_id': 'EU02785522',
  'session_id': '2021-01-05_0001_DE_82a8917d-d9cb-a665-a1ce-34e9a279445c',
  'timestamp': '2021-01-05 13:10:14',
  'channel_label': 'Affiliate',
  'holder_engagement': 1,
  'closer_engagement': 0,
  'conversion': 0,
  'impression_interaction': 0},
 {'conversion_id': 'EU02785522',
  'session_id': '2021-01-10_0001_DE_82a8917d-d9cb-a665-a1ce-34e9a279445c',
  'timestamp': '2021-01-10 16:58:36',
  'channel_label': 'SEA - Brand',
  'holder_engagement': 1,
  'closer_engagement': 0,
  'conversion': 0,
  'impression_interaction': 0},
 {'conversion_id': 'EU02785522',
  'session_id': '2021-01-10_0002_DE_82a8917d-d9cb-a665-a1ce-34e9a279445c',
  'timestamp': '2021-01-10 17:09:33',
  'channel_label': 'Email',
  'holder_engagement': 1,
  'closer_engagement': 1,
  'conversion': 1,
  'impression_interaction': 0}]
  
  
redistribution_parameter = {
    'initializer' : {
        'direction' : 'earlier_sessions_only',
        'receive_threshold' : 0,
        'redistribution_channel_labels' : ['Direct', 'Email_Newsletter'],
    },
    'holder' : {
        'direction' : 'any_session',
        'receive_threshold' : 0,
        'redistribution_channel_labels' : ['Direct', 'Email_Newsletter'],
    },
    'closer' : {
        'direction' : 'later_sessions_only',
        'receive_threshold' : 0.1,
        'redistribution_channel_labels' : ['SEO - Brand'],
    }
}

# Insert API Key here
api_key = 'ae3e7602-f3e9-47c0-8917-796940dbc28f'

# Insert Conversion Type ID here
conv_type_id = 'data_engineering_challenge'

api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={conv_type_id}"

body = {
    'customer_journeys': customer_journeys,
    'redistribution_parameter': redistribution_parameter
}

headers = {
    'Content-Type': 'application/json',
    'x-api-key': api_key
}

response = requests.post(api_url, data=json.dumps(body), headers=headers)

results = response.json()

print(f"Status Code: {results['statusCode']}")

print("-"*30)

print(f"Partial Failure Errors: {results['partialFailureErrors']}")

print("-"*30)

print(results['value'])
