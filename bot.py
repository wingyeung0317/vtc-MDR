import discord
import config
from discord.ext import commands, tasks
from discord import app_commands
import vtcics
import asyncio
import pandas as pd

def run_discord_bot():
    TOKEN = config.token
    bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())
    # client = discord.Client(intents=discord.Intents.all())

    @tasks.loop(seconds=300)
    async def auto_send():
        for index, announce_channel in vtcics.grab_announce_channel().iterrows():
            print(announce_channel['id'])
            channel = await bot.fetch_channel(announce_channel['id'])
            await channel.send('Test message')
        for index, announce_user in vtcics.grab_announce_user().iterrows():
            print(announce_user['id'])
            user = await bot.fetch_user(announce_user['id'])
            await user.send('Test message')

    @tasks.loop(seconds=300)
    async def auto_sync(guildDF:pd.DataFrame, userDF:pd.DataFrame):
        vtcics.sync_guild(guildDF, userDF)

    @bot.event
    async def on_ready():
        print(f"{bot.user} is now running!")
        try:
            syncedCMD = await bot.tree.sync()
            print(f"Synced {len(syncedCMD)} commands")
        except Exception as e:
            print(e)

        guildDF = pd.DataFrame()
        userDF = pd.DataFrame()
        for guild in bot.guilds:
            for channel in guild.text_channels:
                # text_channel_list.append(channel)
                guildDF = pd.concat([guildDF, pd.DataFrame({"id":[channel.id], "name":[channel.name], "guild_id":[guild.id], "guild_name":[guild.name]})]).reset_index(drop=True)
            for user in guild.members:
                userDF = pd.concat([userDF, pd.DataFrame({"user_id":[user.id], "name":[user.name], "guild_id":[guild.id]})])

        if not auto_sync.is_running():
            auto_sync.start(guildDF, userDF)
        
        # user = await bot.fetch_user('525916472833343528')
        # text_channel_list.append(user)

        if not auto_send.is_running():
            auto_send.start()

        print(
            f'{bot.user} has successfully connected to the following guild(s):\n'
            f'{guild.name}(id: {guild.id})'
        )

        await bot.change_presence(
            activity=discord.Activity(name='its creator: killicit.wy', type=discord.ActivityType.listening)
        )
            
    @bot.tree.command(name="test")
    async def test(interaction: discord.Interaction):
        await interaction.response.send_message("Hello World! (via /test)", ephemeral=True)

    @bot.tree.command(name="dm", description="Say \"Hello World\" by sending direct message to you. So you can use the commands by direct message.")
    async def dm(interaction: discord.Interaction):
        await interaction.response.defer()
        asyncio.sleep(3)
        await interaction.user.send("Hello World! (via /dm)")
        await interaction.followup.send("Check your Direct Messages", ephemeral=True)

    @bot.tree.command(name="upload", description="Upload your moodle calendar link (get it at: https://moodle2324.vtc.edu.hk/calendar/export.php) ")
    @app_commands.describe(url = "Paste the calendar link here")
    async def upload(interaction: discord.Interaction, url:str):
        await interaction.response.defer()
        asyncio.sleep(25)
        await interaction.followup.send(f"{vtcics.syncURL(interaction.user.id, interaction.user.name, url)}")
        # await interaction.response.send_message(f"```{vtcics.grabDeadlines(url).loc[:, ['name', 'due']].reset_index(drop=True)}```")

    bot.run(TOKEN)