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
	metrics_worksheet.insert_row(["ID пользователя", "Дата и время", "Метрика", "+", "-", "Участок", "Город"], 1)
name = "USERDATA"
try:
	data_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	data_worksheet = sh.worksheet(name)
	data_worksheet.insert_row(["ID пользователя", "Дата регистрации", "ФИО", "Номер телефона", "Город", "Участок"], 1)
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


my_persistence = PicklePersistence(filename='database.noext')


def get_menu(markers):
	print(markers)
	return [[KeyboardButton(i + "+"), KeyboardButton(i + "-")] for i in markers] + [
		[KeyboardButton("Создать инцидент")],
		[KeyboardButton("Таблица")],
		[KeyboardButton("Изменить метрики")]
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
				metr_worksheet.insert_row(["ID пользователя", "Дата и время", "Метрика", "+", "-", "Участок", "Город"], 1)
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
	context.bot_data[uid]["status"] = "name"
	context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
	update.message.reply_text('Введите, пожалуйста, ФИО', reply_markup=ReplyKeyboardRemove())


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
		update.message.reply_text('Нужно зарегистрироваться. Введите, пожалуйста, ФИО', reply_markup=ReplyKeyboardRemove())
		return
	context.bot_data[uid]["city"] = update.callback_query.data
	context.bot_data[uid]["status"] = "place"
	update.callback_query.edit_message_text('Введите, пожалуйста, участок')


def texter(update, context):
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
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text('Нужно зарегистрироваться. Введите, пожалуйста, ФИО', reply_markup=ReplyKeyboardRemove())
		return
	status = context.bot_data[uid]["status"]
	print(status)
	if status in ["name", "phone_number", "place"]:
		if status == "name":
			name = update.message.text.split()
			if len(name) != 3 or not all(i.isalpha() for i in name):
				update.message.reply_text('Неверный формат ФИО, попробуйте ещё раз')
			else:
				context.bot_data[uid]["name"] = update.message.text
				context.bot_data[uid]["status"] = "phone_number"
				update.message.reply_text('Введите, пожалуйста, номер телефона')
		elif status == "phone_number":
			try:
				phone_number = phonenumbers.parse(update.message.text, None)
				if not phonenumbers.is_valid_number(phone_number):
					update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
				else:
					context.bot_data[uid]["phone_number"] = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
					context.bot_data[uid]["status"] = "city"
					update.message.reply_text('Введите, пожалуйста, город', reply_markup=InlineKeyboardMarkup([
						[InlineKeyboardButton(city, callback_data=city)] for city in context.bot_data["cities"]
					]), one_time_keyboard=True)
			except Exception as e:
				print(e)
				try:
					phone_number = phonenumbers.parse(update.message.text, "RU")
					if not phonenumbers.is_valid_number(phone_number):
						update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
					else:
						context.bot_data[uid]["phone_number"] = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
						context.bot_data[uid]["status"] = "city"
						update.message.reply_text('Введите, пожалуйста, город', reply_markup=InlineKeyboardMarkup([
							[InlineKeyboardButton(city, callback_data=city)] for city in context.bot_data["cities"]
						]), one_time_keyboard=True)
				except Exception as e:
					print(e)
					update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
		elif status == "place":
			context.bot_data[uid]["place"] = update.message.text
			context.bot_data[uid]["status"] = "ready"
			context.bot_data[uid]["markers"] = ["М", "Ж"]
			data_queue.put([uid, str(datetime.now()), context.bot_data[uid]["name"], context.bot_data[uid]["phone_number"], context.bot_data[uid]["city"], context.bot_data[uid]["place"]])
			update.message.reply_text('Регистрация успешно пройдена. Спасибо!', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
	elif status == "admin":
		text = update.message.text.split('\n')
		context.bot_data[uid]["status"] = "ready"
		context.bot_data["cities"] = text
		update.message.reply_text("Список городов обновлён /menu")
	elif status == "sheet":
		text = update.message.text
		if text == 'Отменить привязку таблицы':
			context.bot_data[uid]["status"] = "ready"
			update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
		else:
			context.bot_data[uid]["sheet"] = text
			context.bot_data[uid]["status"] = "ready"
			update.message.reply_text('Таблица успешно привязана', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
	elif status == "markers":
		text = update.message.text
		if text == 'Вернуться в главное меню':
			context.bot_data[uid]["status"] = "ready"
			update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
		else:
			context.bot_data[uid]["markers"] = text.split("\n")
			context.bot_data[uid]["status"] = "ready"
			update.message.reply_text('Метрики успешно обновлены', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
	else:
		text = update.message.text
		if text == "Таблица":
			context.bot_data[uid]["status"] = "sheet"
			if "sheet" in context.bot_data[uid] and context.bot_data[uid]["sheet"]:
				update.message.reply_text(f'Текущая таблица: {context.bot_data[uid]["sheet"]}')
			else:
				update.message.reply_text('Таблица ещё не привязана')
			update.message.reply_text(f'Предоставьте, пожалуйста, доступ к таблице: {GSPREAD_EMAIL}. После этого пришлите ссылку на таблицу', reply_markup=ReplyKeyboardMarkup([
				[KeyboardButton('Отменить привязку таблицы')]
			]), one_time_keyboard=True)
		elif text == "Изменить метрики":
			context.bot_data[uid]["status"] = "markers"
			cur_metr = "\n".join(context.bot_data[uid]["markers"])
			update.message.reply_text(f'Пришлите, пожалуйста, новые метрики, каждую с новой строки. Текущие метрики:\n{cur_metr}', reply_markup=ReplyKeyboardMarkup([
				[KeyboardButton('Вернуться в главное меню')]
			]), one_time_keyboard=True)
		else:
			location_updated = False
			if status == "awaiting_location" and update.message.location:
				location = update.message.location
				if location:
					if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
						update.message.reply_text("У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
						return
					if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
						try:
							context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
						except Exception as e:
							print(e)
							update.message.reply_text("Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
							return
					send_location(uid=uid, longitude=location.longitude, latitude=location.latitude, place=context.bot_data[uid]["place"], city=context.bot_data[uid]["city"], sheet_link=context.bot_data[uid]["sheet"])
					location_updated = True
					context.bot_data[uid]["last_updated_location"] = str(datetime.now())
					context.bot_data[uid]["status"] = "ready"
					update.message.reply_text('Геолокация обновлена', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
			if not location_updated and (datetime.now() - datetime.strptime(context.bot_data[uid]["last_updated_location"], "%Y-%m-%d %H:%M:%S.%f")).seconds >= 1800:
				context.bot_data[uid]["status"] = "awaiting_location"
				update.message.reply_text("Пришлите, пожалуйста, текущую геолокацию", reply_markup=ReplyKeyboardMarkup([
					[KeyboardButton("Отправить", request_location=True)]
				]))
			else:
				if status == "ready":
					text = update.message.text
					marker, sign = text[:-1], text[-1]
					if marker in context.bot_data[uid]["markers"] and sign in ["+", "-"]:
						if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
							update.message.reply_text("У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
							return
						if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
							try:
								context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
							except Exception as e:
								print(e)
								update.message.reply_text("Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
								return
						metrics_queue.put([context.bot_data[uid]["sheet"], [uid, marker, 1 if sign == "+" else 0, 1 if sign == "-" else 0, context.bot_data[uid]["place"], context.bot_data[uid]["city"]]])
						update.message.reply_text("Счётчик успешно обновлён")
					elif text == "Создать инцидент":
						if "sheet" not in context.bot_data[uid] or not context.bot_data[uid]["sheet"]:
							update.message.reply_text("У вас пока нет действующей таблицы (spreadsheets.google.com). Привяжите её главном меню", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
							return
						if "sh" not in context.bot_data[uid] or not context.bot_data[uid]["sh"]:
							try:
								context.bot_data[uid]["sh"] = gc.open_by_url(context.bot_data[uid]["sheet"])
							except Exception as e:
								print(e)
								update.message.reply_text("Бот не может получить доступ к таблице. Пожалуйста, проверьте настройки доступа", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])))
								return
						context.bot_data[uid]["status"] = "confirm_vote"
						context.bot_data[uid]["confirmation_number"] = random.randint(1000, 30000000)
						while context.bot_data[uid]["confirmation_number"] in context.bot_data["used_numbers"]:
							context.bot_data[uid]["confirmation_number"] = random.randint(1000, 30000000)
						update.message.reply_text(f'Пришлите, пожалуйста, фотографию. Ваш код подтвержения: {context.bot_data[uid]["confirmation_number"]}', reply_markup=ReplyKeyboardMarkup([
							[KeyboardButton(f'Отменить подтверждение {context.bot_data[uid]["confirmation_number"]}')]
						]), one_time_keyboard=True)
				elif status == "confirm_vote":
					text = ''
					print(12345)
					try:
						text = update.message.text
						if text == f'Отменить подтверждение {context.bot_data[uid]["confirmation_number"]}':
							context.bot_data[uid]["status"] = "ready"
							context.bot_data[uid]["confirmation_number"] = -1
							update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)
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
								update.message.reply_text(f"""
								Подтверждение отправлено, спасибо! Ваш номер: `{context.bot_data[uid]["confirmation_number"]}`
								""", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True, parse_mode=ParseMode.MARKDOWN)
								context.bot_data[uid]["confirmation_number"] = -1
						except Exception:
							pass


def stop(update, context):
	if str(update.message.chat_id) == "814961422":
		os.kill(os.getpid(), signal.SIGINT)
		exit()


def admin(update, context):
	global LOADED_DUMP
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
		update.message.reply_text('Нужно зарегистрироваться. Введите, пожалуйста, ФИО', reply_markup=ReplyKeyboardRemove())
		return
	if uid in ["814961422", "106052"]:
		context.bot_data[uid]["status"] = "admin"
		s = '\n'.join(context.bot_data["cities"]) if "cities" in context.bot_data else ""
		update.message.reply_text('Вот список городов:')
		update.message.reply_text(s if s else "Городов пока нет")
		update.message.reply_text('''
			/menu - вернуться в меню
			Или отправьте новый список городов, по одному в каждой строке
		''')
	else:
		update.message.reply_text("Вы - не администратор")


def menu(update, context):
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
		context.bot_data[uid]["status"] = "name"
		context.bot_data[uid]["last_updated_location"] = str(datetime.now() - timedelta(hours=1))
		update.message.reply_text('Нужно зарегистрироваться. Введите, пожалуйста, ФИО', reply_markup=ReplyKeyboardRemove())
		return
	if "status" not in context.bot_data[uid] or context.bot_data[uid]["status"] not in ["ready", "admin", "create_incident"]:
		update.message.reply_text('Вы не закончили регистрацию, нажмите /start')
	else:
		context.bot_data[uid]["status"] = "ready"
		update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[uid]["markers"])), one_time_keyboard=True)


def save_data(update, context):
	if str(update.message.chat_id) in ["814961422", "106052"]:
		with open("bot_data.json", "w+", encoding="utf-8") as f:
			s = context.bot_data
			for i in s:
				if "sh" in s[i]:
					s[i]["sh"] = ""
				if "metrics_worksheet" in s[i]:
					s[i]["metrics_worksheet"] = ""
				if "locations_worksheet" in s[i]:
					s[i]["locations_worksheet"] = ""
			dump(context.bot_data, f, ensure_ascii=False, indent=4)
			update.message.reply_text("Данные успешно сохранены", reply_markup=ReplyKeyboardMarkup(get_menu(context.bot_data[str(update.message.chat_id)]["markers"])), one_time_keyboard=True)
	else:
		update.message.reply_text("Вы - не администратор")


def save_jobs(update, context):
	global JOBS_ALLOWED
	if str(update.message.chat_id) in ["814961422", "106052"]:
		attempts = 0
		JOBS_ALLOWED = False
		if attempts == 4:
			update.message.reply_text("Не удалось сохранить очереди")
		else:
			with open("job_data.json", "w+", encoding="utf-8") as f:
				s = dict()
				s["s3_queue"] = list(s3_queue.queue)
				s["metrics_queue"] = list(metrics_queue.queue)
				s["data_queue"] = list(data_queue.queue)
				s["incidents_queue"] = list(incidents_queue.queue)
				s["locations_queue"] = list(locations_queue.queue)
				dump(s, f, ensure_ascii=False, indent=4)
		update.message.reply_text("Очереди сохранены")
	else:
		update.message.reply_text("Вы - не администратор")


def stop_updaters(update, context):
	global JOBS_ALLOWED
	if str(update.message.chat_id) in ["814961422", "106052"]:
		JOBS_ALLOWED = False
		update.message.reply_text("Загрузки приостановлены")
	else:
		update.message.reply_text("Вы - не администратор")


def resume_updaters(update, context):
	global JOBS_ALLOWED
	if str(update.message.chat_id) in ["814961422", "106052"]:
		JOBS_ALLOWED = True
		update.message.reply_text("Загрузки возобновлены")
	else:
		update.message.reply_text("Вы - не администратор")


def main():
	updater = Updater("1361720678:AAFxLegEiYeIf2wOCusSPD4uN7caA6InDC4", use_context=True, persistence=my_persistence)
	dp = updater.dispatcher
	dp.add_handler(CommandHandler("start", start))
	dp.add_handler(CommandHandler("shutdown", stop))
	dp.add_handler(CommandHandler("admin", admin))
	dp.add_handler(CommandHandler("menu", menu))
	dp.add_handler(CommandHandler("save_data", save_data))
	dp.add_handler(CommandHandler("save_jobs", save_jobs))
	dp.add_handler(CommandHandler("stop_updaters", stop_updaters))
	dp.add_handler(CommandHandler("resume_updaters", resume_updaters))
	dp.add_handler(CallbackQueryHandler(button))
	dp.add_handler(MessageHandler(Filters.all, texter))
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
