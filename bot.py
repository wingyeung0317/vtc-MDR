import discord
import config
from discord.ext import commands
from discord import app_commands
import vtcics
import asyncio

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
        await interaction.response.defer()
        asyncio.sleep(25)
        await interaction.followup.send(f"{vtcics.sync(interaction.user.id, interaction.user.name, url)}")
        # await interaction.response.send_message(f"```{vtcics.grabDeadlines(url).loc[:, ['name', 'due']].reset_index(drop=True)}```")

    bot.run(TOKEN)