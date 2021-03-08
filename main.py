import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
import traceback
from telegram.ext import CallbackQueryHandler, PicklePersistence, Updater, CommandHandler, MessageHandler, Filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ParseMode, ReplyKeyboardRemove
from time import sleep
import random
import string
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import os
import signal
import queue
import schedule
from json import load, dump
import phonenumbers
from sys import exit
import os


s3_queue = queue.Queue()
metrics_queue = queue.Queue()
data_queue = queue.Queue()
locations_queue = queue.Queue()
incidents_queue = queue.Queue()
s3_client = boto3.client('s3')
confirmation_numbers = set(range(100, 1000000))
phrases = {}


def get_translation(phrase, lang="ru"):
	if lang == "ru":
		return phrase
	return phrases[phrase]


def upload_file(file_name):
	s3_queue.put(file_name)
	print("pushed", file_name)
	return True


with open("job_data.json", "r", encoding="utf-8") as f:
	s = load(f)
	s3_queue.queue = queue.deque(s["s3_queue"])
	metrics_queue.queue = queue.deque(s["metrics_queue"])
	data_queue.queue = queue.deque(s["data_queue"])
	locations_queue.queue = queue.deque(s["locations_queue"])
	incidents_queue.queue = queue.deque(s["incidents_queue"])


LOADED_DUMP = False
JOBS_ALLOWED = True
GSPREAD_EMAIL = "visior-bot@active-area-251510.iam.gserviceaccount.com"
gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', ['https://spreadsheets.google.com/feeds']))
admin_gspread_link = "https://docs.google.com/spreadsheets/d/1oBuw3eANvCYhPjalKgRvJXElSZ5IjUssY58yMhBK_n4/edit?usp=sharing"
sh = gc.open_by_url(admin_gspread_link)
name = "METRICS"
try:
	metrics_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	metrics_worksheet = sh.worksheet(name)
	metrics_worksheet.insert_row(["ID пользователя", "Дата и время", "Метрика", "+", "-", "Место замера", "Город"], 1)
name = "USERDATA"
try:
	data_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	data_worksheet = sh.worksheet(name)
	data_worksheet.insert_row(["ID пользователя", "Дата регистрации", "Город", "Место замера", "Месяц", "День"], 1)
name = "LOCATIONS"
try:
	locations_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	locations_worksheet = sh.worksheet(name)
	locations_worksheet.insert_row(["ID пользователя", "Дата и время", "Широта", "Долгота", "Место", "Город"], 1)
name = "INCIDENTS"
try:
	incidents_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	incidents_worksheet = sh.worksheet(name)
	incidents_worksheet.insert_row(["ID пользователя", "Дата и время", "Файл", "Номер подтверждения", "Место", "Город"], 1)


def get_menu(markers, lang):
	global phrases
	phrases["Создать инцидент"] = "Create Incident"
	phrases["Таблица"] = "Spreadsheet"
	phrases["Изменить метрики"] = "Change metrics"
	print(markers)
	return [[KeyboardButton(i + "+"), KeyboardButton(i + "-")] for i in markers] + [
		[KeyboardButton(get_translation("Создать инцидент", lang))],
		[KeyboardButton(get_translation("Таблица", lang))],
		[KeyboardButton(get_translation("Изменить метрики", lang))]
	]


def push_s3_job():
	print("gotta push s3")
	while not s3_queue.empty():
		print("getting...")
		file_name = s3_queue.get()
		print(file_name)
		try:
			response = s3_client.upload_file(file_name, "statpad-logs", file_name, ExtraArgs={'ACL':'public-read'})
			os.remove(file_name)
			print("success", file_name)
		except ClientError as e:
			print(e)
			print("nope, pushing again", file_name)
			s3_queue.put(file_name)
	print("done, s3 empty")


def push_metrics_job():
	print("gotta push metrics")
	pushes = dict()
	while JOBS_ALLOWED and not metrics_queue.empty():
		print("getting... metrics")
		row = metrics_queue.get()
		print("got", row)
		sheet_link, data = row
		uid, marker, plus, minus, place, city = data
		idd = uid + "_" + marker
		if idd in pushes:
			pushes[idd]["+"] += plus
			pushes[idd]["-"] += minus
		else:
			pushes[idd] = {
				"uid": uid,
				"marker": marker,
				"+": plus,
				"-": minus,
				"place": place,
				"city": city,
				"sheet_link": sheet_link
			}
	for i in pushes:
		data_row = [pushes[i]["marker"], pushes[i]["+"], pushes[i]["-"], pushes[i]["place"], pushes[i]["city"]]
		try:
			print(metrics_worksheet)
			print(pushes[i]["sheet_link"])
			metrics_worksheet.insert_row([pushes[i]["uid"], str(datetime.now())] + data_row, 2)
			sheet_link = pushes[i]["sheet_link"]
			sh = gc.open_by_url(sheet_link)
			name = "METRICS"
			try:
				metr_worksheet = sh.worksheet(name)
			except Exception as e:
				sh.add_worksheet(title=name, rows="5", cols="20")
				metr_worksheet = sh.worksheet(name)
				metr_worksheet.insert_row(["ID пользователя", "Дата и время", "Метрика", "+", "-", "Место замера", "Город"], 1)
			metr_worksheet.insert_row([pushes[i]["uid"], str(datetime.now())] + data_row, 2)
			print("success")
		except Exception as e:
			print(e)
			row_toinsert = [pushes[i]["sheet_link"], [pushes[i]["uid"]] + data_row]
			print("failed, pushing back", row_toinsert)
			metrics_queue.put(row_toinsert)
	print("done, metrics empty")


def push_data_job():
	print("gotta push data")
	while JOBS_ALLOWED and not data_queue.empty():
		row = data_queue.get()
		print("got", row)
		try:
			data_worksheet.insert_row(row, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			data_queue.put(row)
	print("done, data empty")


def push_locations_job():
	print("gotta push locations")
	while JOBS_ALLOWED and not locations_queue.empty():
		print("getting... locations")
		row = locations_queue.get()
		sheet_link, data = row
		print("got", row)
		try:
			locations_worksheet.insert_row(data, 2)
			sh = gc.open_by_url(sheet_link)
			name = "LOCATIONS"
			try:
				loc_worksheet = sh.worksheet(name)
			except Exception as e:
				sh.add_worksheet(title=name, rows="5", cols="20")
				loc_worksheet = sh.worksheet(name)
				loc_worksheet.insert_row(["ID пользователя", "Дата и время", "Широта", "Долгота", "Место", "Город"], 1)
			loc_worksheet.insert_row(data, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			locations_queue.put(row)
	print("done, locations empty")


def push_incidents_job():
	print("gotta push incidents")
	while JOBS_ALLOWED and not incidents_queue.empty():
		print("getting... incidents")
		row = incidents_queue.get()
		sheet_link, data = row
		print("got", row)
		try:
			incidents_worksheet.insert_row(data, 2)
			sh = gc.open_by_url(sheet_link)
			name = "INCIDENTS"
			try:
				inc_worksheet = sh.worksheet(name)
			except Exception as e:
				sh.add_worksheet(title=name, rows="5", cols="20")
				inc_worksheet = sh.worksheet(name)
				inc_worksheet.insert_row(["ID пользователя", "Дата и время", "Файл", "Номер подтверждения", "Место", "Город"], 1)
			inc_worksheet.insert_row(data, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			incidents_queue.put(row)
	print("done, incidents empty")


def send_location(uid, longitude, latitude, place, city, sheet_link):
	place = place.strip("'")
	if place.isdigit():
		place = int(place)
	locations_queue.put([sheet_link, [uid, str(datetime.now()), longitude, latitude, place, city]])


def start(update, context):
	global phrases
	global LOADED_DUMP
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.message.chat_id)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
	context.bot_data[uid]["status"] = "lang"
	context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
	update.message.reply_text("Hi!", reply_markup=InlineKeyboardMarkup([
		[InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en')],
		[InlineKeyboardButton("🇷🇺", callback_data='ru')]
	]))
	save_data(update, context)
	save_jobs(update, context)


def button(update, context):
	global LOADED_DUMP
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.callback_query.from_user.id)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.callback_query.edit_message_text("Hi!", reply_markup=InlineKeyboardMarkup([
			InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en'),
			InlineKeyboardButton("🇷🇺", callback_data='ru')
		]))
		return
	data = update.callback_query.data
	if data == "en":
		context.bot_data[uid]["lang"] = "en"
		context.bot_data[uid]["markers"] = ["М", "F"]
	else:
		context.bot_data[uid]["lang"] = "ru"
		context.bot_data[uid]["markers"] = ["М", "Ж"]
	phrases["Введите, пожалуйста, город"] = "Please, enter your city"
	context.bot_data[uid]["status"] = "city"
	update.callback_query.edit_message_text(get_translation('Введите, пожалуйста, город', context.bot_data[uid]["lang"]))
	save_data(update, context)
	save_jobs(update, context)


def texter(update, context):
	global LOADED_DUMP, phrases
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.message.chat_id)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text("Hi!", reply_markup=InlineKeyboardMarkup([
			InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en'),
			InlineKeyboardButton("🇷🇺", callback_data='ru')
		]))
		return
	status = context.bot_data[uid]["status"]
	print(status)
	if status == "city":
		phrases['Введите, пожалуйста, место замера'] = "Enter place name"
		context.bot_data[uid]["city"] = update.message.text
		context.bot_data[uid]["status"] = "place"
		update.message.reply_text(get_translation('Введите, пожалуйста, место замера', context.bot_data[uid]["lang"]))
	elif status == "place":
		context.bot_data[uid]["place"] = update.message.text
		context.bot_data[uid]["status"] = "ready"
		data_queue.put([uid, str(datetime.now()), context.bot_data[uid]["city"], context.bot_data[uid]["place"], str(datetime.now().month), str(datetime.now().day)])
		phrases['Регистрация успешно пройдена. Спасибо!'] = "Registration is successful. Enjoy the app!"
		print("markers", context.bot_data[uid]["markers"], type(context.bot_data[uid]["markers"]))
		print("lang", context.bot_data[uid]["lang"], type(context.bot_data[uid]["lang"]))
		update.message.reply_text(get_translation('Регистрация успешно пройдена. Спасибо!', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
	elif status == "admin":
		text = update.message.text.split('\n')
		context.bot_data[uid]["status"] = "ready"
		context.bot_data["cities"] = text
		phrases['Список городов обновлён /menu!'] = "Cities list updated /menu"
		update.message.reply_text(get_translation('Регистрация успешно пройдена. Спасибо!', context.bot_data[uid]["lang"]))
	elif status == "sheet":
		text = update.message.text
		phrases['Список городов обновлён /menu!'] = "Cities list updated /menu"
		if text == 'Отменить привязку таблицы' or text == "Cancel":
			context.bot_data[uid]["status"] = "ready"
			phrases['Главное меню'] = "Main menu"
			update.message.reply_text(get_translation('Главное меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
		else:
			context.bot_data[uid]["sheet"] = text
			context.bot_data[uid]["status"] = "ready"
			phrases['Таблица успешно привязана'] = "Spreadsheet linked successfully"
			update.message.reply_text(get_translation('Таблица успешно привязана', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
	elif status == "markers":
		text = update.message.text
		if text == 'Вернуться в главное меню' or text == "To main menu":
			context.bot_data[uid]["status"] = "ready"
			phrases['Главное меню'] = "Main menu"
			update.message.reply_text(get_translation('Главное меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
		else:
			context.bot_data[uid]["markers"] = text.split("\n")
			context.bot_data[uid]["status"] = "ready"
			phrases['Метрики успешно обновлены'] = "Metrics updated successfully"
			update.message.reply_text(get_translation('Метрики успешно обновлены', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
	elif status == "message":
		text = update.message.text
		update.message.reply_text('Сообщение разослано' if context.bot_data[uid]["lang"] == "ru" else "Messages sent")
		for i in context.bot_data:
			if "status" in i:
				context.bot.send_message(i, text)
	else:
		text = update.message.text
		if text == "Таблица" or text == "Spreadsheet":
			context.bot_data[uid]["status"] = "sheet"
			if "sheet" in context.bot_data[uid] and context.bot_data[uid]["sheet"]:
				phrases['Текущая таблица:'] = "Current spreadsheet:"
				update.message.reply_text(get_translation('Текущая таблица:', context.bot_data[uid]["lang"]) + f' {context.bot_data[uid]["sheet"]}')
			else:
				phrases['Таблица ещё не привязана'] = "No spreadsheet yet"
				update.message.reply_text(get_translation('Таблица ещё не привязана', context.bot_data[uid]["lang"]))
			phrases['Предоставьте, пожалуйста, доступ к таблице:'] = "Please allow edit access to:"
			phrases['После этого пришлите ссылку на таблицу'] = "Then send a link to the sheet at sheets.google.com"
			phrases['Отменить привязку таблицы'] = "Cancel"
			update.message.reply_text(get_translation('Предоставьте, пожалуйста, доступ к таблице:', context.bot_data[uid]["lang"]) + f' {GSPREAD_EMAIL}. ' + get_translation('После этого пришлите ссылку на таблицу', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup([
				[KeyboardButton(get_translation('Отменить привязку таблицы', context.bot_data[uid]["lang"]))]
			]), one_time_keyboard=True)
		elif text == "Изменить метрики" or text == "Change metrics":
			context.bot_data[uid]["status"] = "markers"
			cur_metr = "\n".join(context.bot_data[uid]["markers"])
			phrases['Пришлите, пожалуйста, новые метрики, каждую с новой строки. Текущие метрики:'] = "Please, send new metrics, each from new line. Current metrics:"
			phrases['Вернуться в главное меню'] = "To main menu"
			update.message.reply_text(get_translation('Пришлите, пожалуйста, новые метрики, каждую с новой строки. Текущие метрики:', context.bot_data[uid]["lang"]) + f'\n{cur_metr}', reply_markup=ReplyKeyboardMarkup([
				[KeyboardButton(get_translation('Вернуться в главное меню', context.bot_data[uid]["lang"]))]
			]), one_time_keyboard=True)
		else:
			location_updated = False
			if status == "awaiting_location" and update.message.location:
				location = update.message.location
				if location:
					if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
						phrases['У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню'] = "You have no current spreadsheet (spreadsheets.google.com). Please link one in main menu"
						print("markers", context.bot_data[uid]["markers"], type(context.bot_data[uid]["markers"]))
						print("lang", context.bot_data[uid]["lang"], type(context.bot_data[uid]["lang"]))
						update.message.reply_text(get_translation('У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
						return
					if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
						try:
							context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
						except Exception as e:
							print(e)
							phrases['Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа'] = "Bot has no edit access to the sheet. Please check your permissions"
							update.message.reply_text(get_translation('Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
							return
					send_location(uid=uid, longitude=location.longitude, latitude=location.latitude, place=context.bot_data[uid]["place"], city=context.bot_data[uid]["city"], sheet_link=context.bot_data[uid]["sheet"])
					location_updated = True
					context.bot_data[uid]["last_updated_location"] = str(datetime.now())
					context.bot_data[uid]["status"] = "ready"
					phrases['Геолокация обновлена'] = "Location updated"
					update.message.reply_text(get_translation('Геолокация обновлена', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
			if not location_updated and (datetime.now() - datetime.strptime(context.bot_data[uid]["last_updated_location"], "%Y-%m-%d %H:%M:%S.%f")).seconds >= 1800:
				context.bot_data[uid]["status"] = "awaiting_location"
				phrases['Пришлите, пожалуйста, текущую геолокацию'] = "Please send your location"
				phrases['Отправить'] = "Send"
				update.message.reply_text(get_translation('Пришлите, пожалуйста, текущую геолокацию', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup([
					[KeyboardButton(get_translation('Отправить', context.bot_data[uid]["lang"]), request_location=True)]
				]))
			else:
				if status == "ready":
					text = update.message.text
					marker, sign = text[:-1], text[-1]
					if marker in context.bot_data[uid]["markers"] and sign in ["+", "-"]:
						if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
							phrases['У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню'] = "You have no current spreadsheet (spreadsheets.google.com). Please link one in main menu"
							update.message.reply_text(get_translation('У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
							return
						if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
							try:
								context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
							except Exception as e:
								print(e)
								phrases['Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа'] = "Bot has no edit access to the sheet. Please check your permissions"
								update.message.reply_text(get_translation('Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
								return
						metrics_queue.put([context.bot_data[uid]["sheet"], [uid, marker, 1 if sign == "+" else 0, 1 if sign == "-" else 0, context.bot_data[uid]["place"], context.bot_data[uid]["city"]]])
						phrases['Счётчик успешно обновлён'] = "Counter updated"
						update.message.reply_text(get_translation('Счётчик успешно обновлён', context.bot_data[uid]["lang"]))
					elif text == "Создать инцидент" or text == "Create Incident":
						if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
							phrases['У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню'] = "You have no current spreadsheet (spreadsheets.google.com). Please link one in main menu"
							update.message.reply_text(get_translation('У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])))
							return
						if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
							try:
								context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
							except Exception as e:
								print(e)
								phrases['Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа'] = "Bot has no edit access to the sheet. Please check your permissions"
								update.message.reply_text(get_translation('Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа', context.bot_data[uid]["lang"]))
								return
						context.bot_data[uid]["status"] = "confirm_vote"
						context.bot_data[uid]["confirmation_number"] = random.randint(1000, 30000000)
						while context.bot_data[uid]["confirmation_number"] in context.bot_data["used_numbers"]:
							context.bot_data[uid]["confirmation_number"] = random.randint(1000, 30000000)
						phrases['Пришлите, пожалуйста, фотографию. Ваш код подтвержения:'] = "Please send the photo. Your confirmation code:"
						phrases['Отменить подтверждение'] = "Cancel"
						update.message.reply_text(get_translation('Пришлите, пожалуйста, фотографию. Ваш код подтвержения:', context.bot_data[uid]["lang"]) + f' {context.bot_data[uid]["confirmation_number"]}', reply_markup=ReplyKeyboardMarkup([
							[KeyboardButton(get_translation('Отменить подтверждение', context.bot_data[uid]["lang"]) + f' {context.bot_data[uid]["confirmation_number"]}')]
						]), one_time_keyboard=True)
				elif status == "confirm_vote":
					text = ''
					print(12345)
					try:
						text = update.message.text
						if text == f'Отменить подтверждение {context.bot_data[uid]["confirmation_number"]}' or text == f'Cancel {context.bot_data[uid]["confirmation_number"]}':
							context.bot_data[uid]["status"] = "ready"
							context.bot_data[uid]["confirmation_number"] = -1
							phrases['Главное меню'] = "Main menu"
							update.message.reply_text(get_translation('Главное меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
					except Exception as e:
						print(e)
					if not text:
						try:
							photo = update.message.photo[-1]
							if photo:
								filename = f"{uid}-{context.bot_data[uid]['confirmation_number']}-{photo.file_id}.jpg"
								photo.get_file().download(filename)
								upload_file(filename)
								incidents_queue.put([context.bot_data[uid]["sheet"], [uid, str(datetime.now()), f"https://statpad-logs.s3.amazonaws.com/{filename}", context.bot_data[uid]["confirmation_number"], context.bot_data[uid]["place"], context.bot_data[uid]["city"]]])
								context.bot_data[uid]["status"] = "ready"
								print("doing!!!")
								context.bot_data["used_numbers"].append(context.bot_data[uid]["confirmation_number"])
								print("done!!!")
								phrases['Подтверждение отправлено, спасибо! Ваш номер:'] = "Confirmation sent. Your number:"
								update.message.reply_text(get_translation('Подтверждение отправлено, спасибо! Ваш номер:', context.bot_data[uid]["lang"]) + str(context.bot_data[uid]["confirmation_number"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True, parse_mode=ParseMode.MARKDOWN)
								context.bot_data[uid]["confirmation_number"] = -1
						except Exception:
							pass
	save_data(update, context)
	save_jobs(update, context)


def stop(update, context):
	if str(update.message.chat_id) == "1389478411":
		os.kill(os.getpid(), signal.SIGINT)
		exit()


def admin(update, context):
	global LOADED_DUMP, phrases
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.message.chat_id)
	print(uid)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text("Hi!", reply_markup=InlineKeyboardMarkup([
			InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en'),
			InlineKeyboardButton("🇷🇺", callback_data='ru')
		]))
		return
	if uid in ["979206581", "106052", "1389478411"]:
		context.bot_data[uid]["status"] = "admin"
		s = '\n'.join(context.bot_data["cities"]) if "cities" in context.bot_data else ""
		phrases['Вот список городов:'] = "The cities list:"
		update.message.reply_text(get_translation('Вот список городов:', context.bot_data[uid]["lang"]))
		update.message.reply_text(s if s else ("Городов пока нет" if context.bot_data[uid]["lang"] == "ru" else "No cities yet"))
		phrases['/menu - вернуться в меню\nИли отправьте новый список городов, по одному в каждой строке'] = "/menu - return to menu\nOr send a new cities list, each on new line"
		update.message.reply_text(get_translation('/menu - вернуться в меню\nИли отправьте новый список городов, по одному в каждой строке', context.bot_data[uid]["lang"]))
	else:
		phrases['Вы - не администратор'] = "You're not admin"
		update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[uid]["lang"]))


def menu(update, context):
	global LOADED_DUMP, phrases
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.message.chat_id)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text("Hi!", reply_markup=InlineKeyboardMarkup([
			InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en'),
			InlineKeyboardButton("🇷🇺", callback_data='ru')
		]))
		return
	if "status" not in context.bot_data[uid] or context.bot_data[uid]["status"] not in ["ready", "admin", "create_incident", "awaiting_location"]:
		phrases['Вы не закончили регистрацию, нажмите /start'] = "You haven't finished setup, please /start again"
		update.message.reply_text(get_translation('Вы не закончили регистрацию, нажмите /start', context.bot_data[uid]["lang"]))
	else:
		context.bot_data[uid]["status"] = "ready"
		phrases['Главное меню'] = "Main menu"
		update.message.reply_text(get_translation('Главное меню', context.bot_data[uid]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"], context.bot_data[uid]["lang"])), one_time_keyboard=True)
	save_data(update, context)
	save_jobs(update, context)


def save_data(update, context):
	global phrases
	print(update.message.chat_id)
	# if str(update.message.chat_id) in ["979206581", "106052", "1389478411"]:
	with open("bot_data.json", "w+", encoding="utf-8") as f:
		s = context.bot_data
		for i in s:
			if "sh" in s[i]:
				s[i]["sh"] = ""
		dump(context.bot_data, f, ensure_ascii=False, indent=4)
		# phrases['Данные успешно сохранены'] = "Dump saved"
		# update.message.reply_text(get_translation('Данные успешно сохранены', context.bot_data[str(update.message.chat_id)]["lang"]), reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[str(update.message.chat_id)]["markers"], context.bot_data[str(update.message.chat_id)]["lang"])), one_time_keyboard=True)
	# else:
		phrases['Вы - не администратор'] = "You're not admin"
		# update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[str(update.message.chat_id)]["lang"]))


def message(update, context):
	global LOADED_DUMP, phrases
	if not LOADED_DUMP:
		with open("bot_data.json", encoding="utf-8") as f:
			s = load(f)
			for i in s:
				context.bot_data[i] = s[i]
			LOADED_DUMP = True
	uid = str(update.message.chat_id)
	if uid not in context.bot_data:
		context.bot_data[uid] = {}
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text("Hi!", reply_markup=InlineKeyboardMarkup([
			InlineKeyboardButton("🇺🇸🇪🇺", callback_data='en'),
			InlineKeyboardButton("🇷🇺", callback_data='ru')
		]))
		return
	if uid in ["979206581", "106052", "1389478411"]:
		context.bot_data[uid]["status"] = "message"
		phrases['Пожалуйста, введите сообщение'] = "Enter the message"
		update.message.reply_text(get_translation('Пожалуйста, введите сообщение', context.bot_data[str(update.message.chat_id)]["lang"]))
	else:
		phrases['Вы - не администратор'] = "You're not admin"
		update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[str(update.message.chat_id)]["lang"]))


def save_jobs(update, context):
	global JOBS_ALLOWED, phrases
	# if str(update.message.chat_id) in ["979206581", "106052", "1389478411"]:
	attempts = 0
	JOBS_ALLOWED = False
	if attempts == 4:
		phrases['Не удалось сохранить очереди'] = "Queues weren't saved"
		# update.message.reply_text(get_translation('Не удалось сохранить очереди', context.bot_data[str(update.message.chat_id)]["lang"]))
	else:
		with open("job_data.json", "w+", encoding="utf-8") as f:
			s = dict()
			s["s3_queue"] = list(s3_queue.queue)
			s["metrics_queue"] = list(metrics_queue.queue)
			s["data_queue"] = list(data_queue.queue)
			s["incidents_queue"] = list(incidents_queue.queue)
			s["locations_queue"] = list(locations_queue.queue)
			dump(s, f, ensure_ascii=False, indent=4)
	# phrases['Очереди сохранены'] = "Queues saved"
		# update.message.reply_text(get_translation('Очереди сохранены', context.bot_data[str(update.message.chat_id)]["lang"]))
	# else:
		# phrases['Вы - не администратор'] = "You're not admin"
		# update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[str(update.message.chat_id)]["lang"]))


def stop_updaters(update, context):
	global JOBS_ALLOWED, phrases
	if str(update.message.chat_id) in ["979206581", "106052", "1389478411"]:
		JOBS_ALLOWED = False
		phrases['Загрузки приостановлены'] = "Uploads paused"
		update.message.reply_text(get_translation('Загрузки приостановлены', context.bot_data[str(update.message.chat_id)]["lang"]))
	else:
		phrases['Вы - не администратор'] = "You're not admin"
		update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[str(update.message.chat_id)]["lang"]))


def resume_updaters(update, context):
	global JOBS_ALLOWED, phrases
	if str(update.message.chat_id) in ["979206581", "106052", "1389478411"]:
		JOBS_ALLOWED = True
		phrases['Загрузки возобновлены'] = "Uploades resumed"
		update.message.reply_text(get_translation('Загрузки возобновлены', context.bot_data[str(update.message.chat_id)]["lang"]))
	else:
		phrases['Вы - не администратор'] = "You're not admin"
		update.message.reply_text(get_translation('Вы - не администратор', context.bot_data[str(update.message.chat_id)]["lang"]))


def main():
	updater = Updater("1283013334:AAHPV7p--L2SfD441bO5cK067RKeRbamsXA", use_context=True)
	dp = updater.dispatcher
	dp.add_handler(CommandHandler("start", start))
	dp.add_handler(CommandHandler("shutdown", stop))
	dp.add_handler(CommandHandler("admin", admin))
	dp.add_handler(CommandHandler("menu", menu))
	dp.add_handler(CommandHandler("save_data", save_data))
	dp.add_handler(CommandHandler("save_jobs", save_jobs))
	dp.add_handler(CommandHandler("stop_updaters", stop_updaters))
	dp.add_handler(CommandHandler("resume_updaters", resume_updaters))
	dp.add_handler(CommandHandler("message", message))
	dp.add_handler(MessageHandler(Filters.all, texter))
	dp.add_handler(CallbackQueryHandler(button))
	updater.start_polling()
	schedule.every().minute.do(push_metrics_job).run()
	schedule.every().minute.do(push_s3_job)
	schedule.every().minute.do(push_data_job)
	schedule.every().minute.do(push_locations_job)
	schedule.every().minute.do(push_incidents_job)
	while True:
		try:
			print(datetime.now())
			for i in schedule.jobs:
				try:
					if i.should_run and JOBS_ALLOWED:
						i.run()
				except Exception as e:
					traceback.print_exc()
			
			sleep(5)
		except Exception as e:
			print(e)


if __name__ == '__main__':
	main()
