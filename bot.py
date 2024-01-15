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

    loop1_sec = 4*60*60
    @tasks.loop(seconds=loop1_sec)
    async def auto_send1():
        for index, announce_channel in vtcics.grab_announce_channel().iterrows():
            channel = await bot.fetch_channel(announce_channel['id'])
            await channel.send('Test message')
        for index, announce_user in vtcics.grab_announce_user().iterrows():
            user = await bot.fetch_user(announce_user['id'])
            (open_events, opened_events, due_events) = vtcics.checkTime(loop1_sec)
            await user.send(vtcics.event_msg(open_events, 'Open', '4 hours'))
            await user.send(vtcics.event_msg(opened_events, 'Opened', '4 hours'))
            await user.send(vtcics.event_msg(due_events, 'Due', '4 hours'))
                
    loop2_sec = 24*60*60
    @tasks.loop(seconds=loop2_sec)
    async def auto_send2():
        for index, announce_channel in vtcics.grab_announce_channel().iterrows():
            channel = await bot.fetch_channel(announce_channel['id'])
            await channel.send('Test message')
        for index, announce_user in vtcics.grab_announce_user().iterrows():
            user = await bot.fetch_user(announce_user['id'])
            (open_events, opened_events, due_events) = vtcics.checkTime(loop2_sec)
            await user.send(vtcics.event_msg(open_events, 'Open', '1 day'))
            await user.send(vtcics.event_msg(opened_events, 'Opened', '1 day'))
            await user.send(vtcics.event_msg(due_events, 'Due', '1 day'))
                
    loop3_sec = 2*24*60*60
    @tasks.loop(seconds=loop3_sec)
    async def auto_send3():
        for index, announce_channel in vtcics.grab_announce_channel().iterrows():
            channel = await bot.fetch_channel(announce_channel['id'])
            await channel.send('Test message')
        for index, announce_user in vtcics.grab_announce_user().iterrows():
            user = await bot.fetch_user(announce_user['id'])
            (open_events, opened_events, due_events) = vtcics.checkTime(loop3_sec)
            await user.send(vtcics.event_msg(open_events, 'Open', '2 days'))
            await user.send(vtcics.event_msg(opened_events, 'Opened', '2 days'))
            await user.send(vtcics.event_msg(due_events, 'Due', '2 days'))

    loop4_sec = 7*24*60*60
    @tasks.loop(seconds=loop4_sec)
    async def auto_send4():
        for index, announce_channel in vtcics.grab_announce_channel().iterrows():
            channel = await bot.fetch_channel(announce_channel['id'])
            await channel.send('Test message')
        for index, announce_user in vtcics.grab_announce_user().iterrows():
            user = await bot.fetch_user(announce_user['id'])
            (open_events, opened_events, due_events) = vtcics.checkTime(loop4_sec)
            await user.send(vtcics.event_msg(open_events, 'Open', '1 week'))
            await user.send(vtcics.event_msg(opened_events, 'Opened', '1 week'))
            await user.send(vtcics.event_msg(due_events, 'Due', '1 week'))

    @tasks.loop(seconds=300)
    async def auto_sync(guildDF:pd.DataFrame, userDF:pd.DataFrame):
        (df, lenDF) = vtcics.sync_guild(guildDF, userDF)
        if len(df)!=0:
            for user_id in df['user_id'].drop_duplicates():
                user = await bot.fetch_user(user_id)
                synced_viewer_df = df[df['user_id']==user_id].drop('user_id', axis = 1)
                event_len = lenDF[lenDF['user_id']==user_id].loc[-1:, 'len'].to_string(index=False)
                print(lenDF[lenDF['user_id']==user_id])
                print(event_len)
                msg = f"{len(synced_viewer_df)} events added (Total {event_len}): ```{synced_viewer_df.to_string(index=False)}```"
                await user.send(msg)

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

        if not auto_send1.is_running():
            auto_send1.start()
        if not auto_send2.is_running():
            auto_send2.start()
        if not auto_send3.is_running():
            auto_send3.start()
        if not auto_send4.is_running():
            auto_send4.start()

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
        await interaction.followup.send(f"{vtcics.syncURL(interaction.user.id, interaction.user.name, url)[0]}")
        # await interaction.response.send_message(f"```{vtcics.grabDeadlines(url).loc[:, ['name', 'due']].reset_index(drop=True)}```")

    @bot.tree.command(name="announce", description="Make this channel announce the events or mute me in this channel")
    async def test(interaction: discord.Interaction):
        await interaction.response.defer()
        asyncio.sleep(25)
        try:
            await interaction.followup.send(f"Will start announce in this channel" if vtcics.mute_channel(interaction.channel.id) else f"Will be muted in this channel")
        except:
            await interaction.followup.send("Failed, not finished this function for user direct messages")

    bot.run(TOKEN)