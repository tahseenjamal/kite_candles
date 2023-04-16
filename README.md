Requirement
Pandas
Redis
Python Kite Client



Python Kite Ticker ---> Redis Queue ---> Consumed by Subcriber [1 minute]

Like this you can have multiple consumers, for 5 minutes candles, 15 minutes and 1 hour

Also note before the Python Kite Ticker code starts, redis hashset should have a key and value of your token.CLIENTID and access token

1. Ensure Redis is running - should be running as a service
2. Ensure before 9am you have started instrument generator program - set it in crontab
3. Ensure userdata file is populated correctly - this is one time activity
4. Ensure set token sets token id and access token as key/value in Redis - this is one time activity - basics is given you need to implement in your token generation program
5. Ensure kite ticker application starts a little before 9am - set it in crontab
6. Ensure 1 minute application inside multi-time is also starts a little before 9am - set it in crontab
