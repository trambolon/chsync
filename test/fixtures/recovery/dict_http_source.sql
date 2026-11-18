CREATE DICTIONARY events.currency_dict (
    code String, rate Float64
)
PRIMARY KEY code
SOURCE(HTTP(URL 'https://rates.example.com/feed' FORMAT 'CSV'))
LIFETIME(3600)
LAYOUT(COMPLEX_KEY_HASHED())
