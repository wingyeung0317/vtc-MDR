import discord
import config
from discord.ext import commands, tasks
from discord import app_commands
import vtcics
import asyncio
import pandas as pd

def run_discord_bot():
    TOKEN = config.token
    bot = commands.Bot(command_prefix="mdr.", intents=discord.Intents.all())
    # client = discord.Client(intents=discord.Intents.all())

    @tasks.loop(seconds=300)
    async def auto_run():
        try:
            await vtcics.auto_send(bot)
        except Exception as e:
            print(e)

    @tasks.loop(seconds=300)
    async def auto_sync(guildDF:pd.DataFrame, userDF:pd.DataFrame):
        try:
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
        except Exception as e:
            print(e)

    @bot.event
    async def on_ready():
        print(f"{bot.user} is now running!")
        try:
            syncedCMD = await bot.tree.sync()
            print(f"Synced {len(syncedCMD)} commands")

            for index, announce_user in vtcics.grab_announce_user().iterrows():
                user = await bot.fetch_user(announce_user['id'])
                await user.send("伺服器遭到重啟, 敬請原諒.")
            for index, announce_channel in vtcics.grab_announce_channel().iterrows():
                channel = await bot.fetch_channel(announce_channel['id'])
                await channel.send("伺服器遭到重啟, 敬請原諒.")

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

            if not auto_run.is_running():
                auto_run.start()

            print(
                f'{bot.user} has successfully connected to the following guild(s):\n'
                f'{guild.name}(id: {guild.id})'
            )

            await bot.change_presence(
                activity=discord.Activity(name='its creator: killicit.wy', type=discord.ActivityType.listening)
            )

        except Exception as e:
            print(e)
            
        print(f"init done!")

    @bot.tree.command(name="test")
    async def test(interaction: discord.Interaction):
        try:
            await interaction.response.send_message("Hello World! (via /test)", ephemeral=True)
        except Exception as e:
            print(e)

    @bot.tree.command(name="dm", description="Say \"Hello World\" by sending direct message to you. So you can use the commands by direct message.")
    async def dm(interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            asyncio.sleep(3)
            await interaction.user.send("Hello World! (via /dm)")
            await interaction.followup.send("Check your Direct Messages", ephemeral=True)
        except Exception as e:
            print(e)

    @bot.tree.command(name="upload", description="Upload your moodle calendar link (get it at: https://moodle2324.vtc.edu.hk/calendar/export.php) ")
    @app_commands.describe(url = "Paste the calendar link here")
    async def upload(interaction: discord.Interaction, url:str):
        try:
            await interaction.response.defer()
            asyncio.sleep(25)
            await interaction.followup.send(f"{vtcics.syncURL(interaction.user.id, interaction.user.name, url)[0]}")
        except Exception as e:
            print(e)
    
    @bot.tree.command(name="deadlines", description="Show a table of all the deadlines")
    async def test(interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            asyncio.sleep(25)
            vtcics.cursor.execute(f"SELECT url FROM users WHERE id = {interaction.user.id}")
            url = vtcics.cursor.fetchone()[0]
            df = vtcics.grabDeadlines(url)
            await interaction.followup.send(f"```{df[df['due'] > vtcics.arrow.now().datetime].loc[:, ['name', 'due']].reset_index(drop=True)}```")
        except Exception as e:
            print(e)
        
    @bot.tree.command(name="events", description="Show a table of all the events")
    async def test(interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            asyncio.sleep(25)
            vtcics.cursor.execute(f"SELECT url FROM users WHERE id = {interaction.user.id}")
            url = vtcics.cursor.fetchone()[0]
            print(url)
            await interaction.followup.send(f"```{vtcics.grabICS(url).loc[:, ['name', 'course']].reset_index(drop=True)}```")
        except Exception as e:
            print(e)
        
    @bot.tree.command(name="open", description="Show a table of all the events open date")
    async def test(interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            asyncio.sleep(25)
            vtcics.cursor.execute(f"SELECT url FROM users WHERE id = {interaction.user.id}")
            url = vtcics.cursor.fetchone()[0]
            df = vtcics.grabOpens(url)
            await interaction.followup.send(f"```{df[df['open'] > vtcics.arrow.now().datetime].loc[:, ['name', 'open']].reset_index(drop=True)}```")
        except Exception as e:
            print(e)

    @bot.tree.command(name="announce", description="Make this channel announce the events or mute me in this channel")
    async def test(interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            asyncio.sleep(25)
            try:
                await interaction.followup.send(f"Will start announce in this channel" if vtcics.mute_channel(interaction.channel.id) else f"Will be muted in this channel")
            except:
                await interaction.followup.send("Failed, not finished this function for user direct messages")
        except Exception as e:
            print(e)
    bot.run(TOKEN)