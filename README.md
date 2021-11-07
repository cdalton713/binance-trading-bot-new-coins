# Automatic New Cryptocurrency Trading Bot

This is a major re-write
of [binance-trading-bot-new-coins](https://github.com/CyberPunkMetalHead/binance-trading-bot-new-coins "binance-trading-bot-new-coins") - credit for the idea goes to him.

In general, the price of a coin immediately increases for a few minutes the moment it is added to a new exchange. This trading bot detects these new coins as soon as they are listed and places buy order as soon as possible.  The bot will then automatically sell the coin based on user defined TP, SL, or TSL percentages as the price of the con changes. Binance and FTX are currently supported. 

Features include:
- Test mode
- Auto sell using Stop Loss, Take Profit, Trailing Stop Loss
- Discord and Telegram notifications
- Optional: Frontload requests towards the beginning of each minute
- Optional: Automatic speed adjustments to avoid API limitations (Binance)

It comes with a live and test mode, so naturally, use it at your own risk.

## Suggested Setup:

Speed is _very_ important for this program to produce successful trades. The setup below is averaging ~0.05 seconds per
iteration - if your local copy is much higher than this I would consider using this set up instead.

1. Create an AWS account
2. **Choose `Asia Pacific (Tokyo) ap-northeast-1` region from menu at the top right**
1. Search for `EC2` in the search bar.
    1. Choose `Ubuntu Server 20.04 LTS (HVM), SSD Volume Type`
    1. Choose `t2.micro`.  This is offered for free from AWS for a year
    1. Choose `Review and Launch` and create the instance
    1. Create an SSH Key.  Download and save somewhere safe
1. Either SSH into instance or choose `connect` from the UI
1. Once in the instance:
   1. Run `sudo apt-get update`
   1. Run `sudo apt-get install git`
   1. Run `sudo apt install python3-pip`
   1. Follow steps in Installation section.

### Don't close the SSH Window!
Closing the SSH window will stop the process.  To keep the program running after closing the SSH window, run the following:
1. Run `sudo apt-get install screen`
2. Run `screen`
3. Run `python3 main.py`
4. Press `ctrl-a` then `ctrl-d` (both windows and mac). This will run the program in the background.
5. To go back, run `screen -r`

## Installation:
- Python 3.7+ is required

1. Git clone the repo: `git clone https://github.com/cdalton713/trading-bot-new-coins`
2. Run `pip install -r requirements.txt`
3. Make copies/rename `auth.example.yml` and `config.example.yml` and edit the files as necessary
4. Run `python main.py`.  Follow any prompts.

### This project uses submodules!
**Why?** I realize this is slightly inconvenient, but I need some parts of this project for other things!  Submodules allow me to share components across different projects.

#### The program _should_ auto install everything for you, but if not:

##### How to Clone With Submodules:

1. `git clone --recurse-submodules https://github.com/cdalton713/trading-bot-new-coins`

##### I Cloned Prior to This Change:

1. cd into directory
2. `git submodule init`
3. `git submodule update`

##### A New Update Is Out:
1. cd into directory
2. `git submodule init`
3. `git submodule update`

## How to Set Up Notifications:
### Discord:
You must have permissions to create a `webhoook` on your chosen Discord server. 

1. Right-click the server icon
2. Go to `Integrations`
3. Click `Create Webhook`
4. Choose an icon (optional), name, and channel
5. Copy the `Webhook URL` and paste into `config.yml` under `NOTIFICATION_OPTIONS -> DISCORD -> AUTH -> ENDPOINT`

### Telegram:
This is a bit more involved than Discord...
1. Go to https://t.me/botfather and add the BotFather Bot
2. Type `/newbot`
3. Choose a name for your bot
4. Choose a username for your bot
5. Carefully copy the `token`
6. Go to the chat you wish the bot to be in and add the bot.  Add Member, then \<your bot name\>
7. Type a message in that chat
8. Visit this url:  `https://api.telegram.org/bot<YourBOTToken>/getUpdates` where `<YourBOTTOKEN>` is your token from step 5.
9. You should see a JSON response which has the chat id displayed.  If the number is negative, keep the minus sign.
10. Copy the token to  `NOTIFICATION_OPTIONS -> TELEGRAM -> AUTH -> ENDPOINT`
11. Copy the chat id to `NOTIFICATION_OPTIONS -> TELEGRAM -> AUTH -> CHAT_ID`

## TODO List:
- Swap to multi-threading?
- ~~Notification service (Discord/Telegram)~~
- Additional tests
- Additional Exchanges*

## I want to contribute:
I would love contributors! Please send a pull request, and I will review it.

*If you plan to add support for additional exchanges, please review and follow the structure used for the FTX and Binance wrapper classes already implemented.

## I found a bug/issue:
Please include any applicable stack traces and logs.  There are currently two log files: `errors.log` and `verbose_log.log`.  Please also attach any relevant information from these files.

## Changelog:
- 2021-11-05:
  1. Various changes/fixes to when-to-sell calculations
  2. Verbose file formatting updated
  3. Additional tests for Binance
  
- 2021-11-04:
  1. Setup Binance TestNet for much better testing.  Functionality seems to be working for each step now based on Binance API responses.
  2. ReadMe and Config Info updated
  
- 2021-10-11:
  1. Binance order price response fix

- 2021-10-05:
  1. Bugs with live Binance fixed
  2. Notifications moved into a submodule for other projects

- 2021-09-30:
  1. Fixed Save issue ([#6](https://github.com/cdalton713/trading-bot-new-coins/issues/6)).
  2. Removed retry option from purchases. Possible fix for [#5](https://github.com/cdalton713/trading-bot-new-coins/issues/5).
  3. Checks version number and notifies if there is a new version.
  
- 2021-09-24: 
  1. `Invalid Symbol` Binance error finally fixed ([#4](https://github.com/cdalton713/trading-bot-new-coins/issues/4))! 
  2. Notifications for Discord and Telegram added
  
- 2021-09-16:
  1. New baseline - I've done a poor job tracking changes thus far.

## Donate:
Made a killing using this?  [Buy me a coffee!](https://venmo.com/u/Cdalton713)
