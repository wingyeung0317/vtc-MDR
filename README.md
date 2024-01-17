# <img src="https://github.com/wingyeung0317/vtc-MDR/assets/121206892/5c30ad26-396b-450e-b97b-10b38661168b" alt="icon" width="32"> vtc-MDR 

VTC Moodle Discord Reminder is a project that uses Discord bot to remind the due dates of VTC Moodle assignments.
1. Add bot to server: [https://discord.com/api/oauth2/authorize?client_id=1194240403008933999&permissions=277025527808&scope=bot](https://discord.com/api/oauth2/authorize?client_id=1194240403008933999&permissions=277025527808&scope=bot)
2. After adding the bot, use `/dm` to let the bot send a direct message to you
3. In DM, use `/upload` url: [with your moodle calendar url](https://moodle2324.vtc.edu.hk/calendar/export.php)
4. use `/announce` in the text channel you want the bot to activate (not necessary, bot would announce you in direct messages anyway and which is the much prefered way too.)
<img src="https://github.com/wingyeung0317/vtc-MDR/assets/121206892/5c30ad26-396b-450e-b97b-10b38661168b" alt="icon" width="512">

## 現有指令
* `/test` : Send hello world to test the bot is working
* `/dm`: Send hello world to user's direct messages
* `/upload`: Upload user's moodle calendar URL to sync events to SQL
* `/events`: Print a table of all events
* `/open`: Print a table of all upcoming events open date
* `/deadlines`: Print a table of all deadlines
* `/announce`: Make the current channel to announce or mute upcoming events

## Build by yourself
1. fork / git clone this project first
2. create a config.py
   ```
   token = [Discord Bot Token]
   engineURL = [Your SQL URL]
   ```
3. run main.py
