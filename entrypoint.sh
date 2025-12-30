#!/bin/bash
set -e

myLogFile=/mnt/hummingbot/logs/entrypoint.log

echo "$(date): ------------------------" >> $myLogFile
echo "$(date): Entrypoint of Hummingbot" >> $myLogFile

# Start Mosquitto MQTT broker in background
echo "$(date): Starting Mosquitto MQTT broker" >> $myLogFile
mosquitto -c /etc/mosquitto/mosquitto.conf -d
sleep 2

# Verify Mosquitto is running
if pgrep mosquitto > /dev/null; then
    MQTT_PID=$(pgrep mosquitto)
    echo "$(date): Mosquitto MQTT broker started successfully (PID: $MQTT_PID)" >> $myLogFile
    
    # Check if it's actually listening on port 1883
    sleep 1
    if grep -q "0100007F:075B" /proc/net/tcp; then
        echo "$(date): Mosquitto is listening on localhost:1883" >> $myLogFile
    else
        echo "$(date): WARNING: Mosquitto running but not listening on localhost:1883!" >> $myLogFile
        echo "$(date): Network table:" >> $myLogFile
        cat /proc/net/tcp >> $myLogFile 2>&1
    fi
    
    # Wait for Mosquitto to be fully ready to accept connections
    sleep 2
    echo "$(date): Mosquitto ready for connections" >> $myLogFile
else
    echo "$(date): ERROR: Mosquitto failed to start!" >> $myLogFile
    echo "$(date): Attempting to start Mosquitto in verbose mode..." >> $myLogFile
    mosquitto -c /etc/mosquitto/mosquitto.conf -v >> $myLogFile 2>&1 &
    sleep 3
    if pgrep mosquitto > /dev/null; then
        echo "$(date): Mosquitto started in verbose mode (PID: $(pgrep mosquitto))" >> $myLogFile
    else
        echo "$(date): CRITICAL: Mosquitto failed to start even in verbose mode!" >> $myLogFile
    fi
fi

echo "$(date): Create symbolic links" >> $myLogFile
ln -sf /mnt/hummingbot/certs /home/hummingbot
ln -sf /mnt/hummingbot/conf /home/hummingbot
ln -sf /mnt/hummingbot/data /home/hummingbot
ln -sf /mnt/hummingbot/logs /home/hummingbot

# Setup the configuration
botDir=`printenv BOT_DIR`

if [[ ! -z $botDir ]]
then
  echo "$(date): 'BOT_DIR' is ${botDir}" >> $myLogFile

  for subDir in connectors controllers scripts strategies
  do
    sourceDir=/home/hummingbot/bots/$botDir/conf/$subDir
    targetDir=/mnt/hummingbot/conf

    if [[ -e $sourceDir ]]
    then
      mkdir -p $targetDir
      echo "$(date): Copy ${sourceDir} to ${targetDir}" >> $myLogFile
      cp -r $sourceDir $targetDir
    fi
  done

  # Copy conf_client.yml if it exists
  confClientSource=/home/hummingbot/bots/$botDir/conf/conf_client.yml
  if [[ -f $confClientSource ]]
  then
    echo "$(date): Copy ${confClientSource} to /home/hummingbot/conf/conf_client.yml" >> $myLogFile
    cp $confClientSource /home/hummingbot/conf/conf_client.yml
  fi
else
  echo "$(date): No 'BOT_DIR' environment variable set" >> $myLogFile
fi

# Start Hummingbot with the strategy
strategyFile=`printenv STRATEGY_FILE`

if [[ ! -z $strategyFile ]]
then
  echo "$(date): 'STRATEGY_FILE' is ${strategyFile}" >> $myLogFile

  password="$(cat /home/hummingbot/conf/.password)"

  bash --login -c "conda activate hummingbot && /home/hummingbot/bin/hummingbot_quickstart.py --headless -p ${password} -f ${strategyFile}"
else
  echo "$(date): No 'STRATEGY_FILE' environment variable set" >> $myLogFile
fi
