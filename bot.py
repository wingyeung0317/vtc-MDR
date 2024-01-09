import discord
import responses
import config
from discord.ext import commands
from discord import app_commands

async def send_message(msg, user_msg, is_private):
    try:
        response = responses.handle_response(user_msg)
        await msg.author.send(response) if is_private else await msg.channel.send(response)
    except Exception as e:
        print(e)

def run_discord_bot():
    TOKEN = config.token
    bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())
    # client = discord.Client(intents=discord.Intents.all())

    @bot.event
    async def on_ready():
        print(f"{bot.user} is now running!")
        try:
            syncedCMD = await bot.tree.sync()
            print(f"Synced {len(syncedCMD)} commands")
        except Exception as e:
            print(e)
            
    @bot.tree.command(name="test")
    async def test(interaction: discord.Interaction):
        await interaction.response.send_message("Hello World! (via /test)", ephemeral=True)   

    @bot.tree.command(name="dm", description="Say \"Hello World\" by sending direct message to you.")
    async def dm(interaction: discord.Interaction):
        await interaction.user.send("Hello World! (via /dm)")

    @bot.tree.command(name="upload", description="Upload your moodle calendar link (get it at: https://moodle2324.vtc.edu.hk/calendar/export.php) ")
    @app_commands.describe(url = "Paste the calendar link here")
    async def upload(interaction: discord.Interaction, url:str):
        await interaction.response.send_message(f"The URL is {url}")


    # """Read msg directly"""
    # @bot.event
    # async def on_message(msg):
    #     if msg.author == bot.user:
    #         return
        
    #     username = str(msg.author)
    #     user_msg = str(msg.content)
    #     channel = str(msg.channel)

    #     print(f"{username} said: {user_msg} ({channel})")

    #     if user_msg[0]=='!':
    #         user_msg = user_msg[1:]
    #         await send_message(msg, user_msg, is_private=1)
    #     else:
    #         await send_message(msg, user_msg, is_private=0)
    # bot.run(TOKEN)