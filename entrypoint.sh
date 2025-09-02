#!/bin/bash
set -e

myLogFile=/mnt/hummingbot/logs/entrypoint.log

echo "$(date): ------------------------" >> $myLogFile
echo "$(date): Entrypoint of Hummingbot" >> $myLogFile

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
