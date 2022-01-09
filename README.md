# Sample Go with InfluxDB

Sample output:

```
❯ go run main.go
Generated 24 data
data successfully inserted
[
 {
  "Measurement": "entity",
  "ID": "3e71bf238b42f5e5caee",
  "Width": 1990454546877802616,
  "Height": 8023763433023687463,
  "Timestamp": "1975-04-24T03:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "3e71bf238b42f5e5caee",
  "Width": 5268878840292321731,
  "Height": 4215897992096951688,
  "Timestamp": "2000-07-27T15:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "3e71bf238b42f5e5caee",
  "Width": 8465060383165737861,
  "Height": 5870848365885998868,
  "Timestamp": "2001-01-26T16:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "3e71bf238b42f5e5caee",
  "Width": 8489296701226432596,
  "Height": 7794621013089894564,
  "Timestamp": "2017-05-17T01:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "3e71bf238b42f5e5caee",
  "Width": 937782269055393912,
  "Height": 7461989135037297579,
  "Timestamp": "2019-11-09T22:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "5b83bc7bda30896abe89",
  "Width": 2914091908790814693,
  "Height": 718132387466562186,
  "Timestamp": "1982-08-02T07:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 1610902500175774985,
  "Height": 3472277501980525118,
  "Timestamp": "1972-12-31T14:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 8537564754276637014,
  "Height": 6369422869469275387,
  "Timestamp": "1987-05-24T19:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 5376487763482572675,
  "Height": 9160630518734755404,
  "Timestamp": "1994-04-28T03:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 1895515831413834873,
  "Height": 5104543216535968093,
  "Timestamp": "1995-06-02T09:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 1526280327873005996,
  "Height": 1753804900844203758,
  "Timestamp": "2003-12-23T07:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 3383023207601895278,
  "Height": 6515434812328279898,
  "Timestamp": "2005-05-08T16:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "aa45dc19edbc87d77400",
  "Width": 9171414336339766130,
  "Height": 1117429866888781200,
  "Timestamp": "2019-12-05T08:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "f8580c6154f799d9816f",
  "Width": 4151173695832385414,
  "Height": 6522517814622321698,
  "Timestamp": "1981-04-22T14:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "f8580c6154f799d9816f",
  "Width": 5171528920688771210,
  "Height": 8565365702207269980,
  "Timestamp": "2002-12-11T00:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "f8580c6154f799d9816f",
  "Width": 119236868681180622,
  "Height": 7222166774931287423,
  "Timestamp": "2003-04-24T00:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "f8580c6154f799d9816f",
  "Width": 3495321220372615768,
  "Height": 5775285320421477900,
  "Timestamp": "2019-07-30T21:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 5653039367274908303,
  "Height": 5313730199876304474,
  "Timestamp": "1980-09-22T15:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 4063269421682053284,
  "Height": 5274989857271059205,
  "Timestamp": "1981-04-03T20:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 744961539668463002,
  "Height": 4383084226311082410,
  "Timestamp": "1987-01-22T11:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 3288269137652788424,
  "Height": 4563741627168244282,
  "Timestamp": "1996-01-14T12:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 2810715318348419570,
  "Height": 512019088416809157,
  "Timestamp": "2000-01-28T00:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 6685060018489253719,
  "Height": 2016459395352568594,
  "Timestamp": "2006-05-21T19:00:00Z"
 },
 {
  "Measurement": "entity",
  "ID": "ff903a1686ac3cf88fb0",
  "Width": 1382192151980548468,
  "Height": 4477479883361370125,
  "Timestamp": "2012-02-05T18:00:00Z"
 }
]
Time wasted for reading the data: 41 ms for data length of 24 and response length of 24
```
