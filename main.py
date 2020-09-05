import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
import traceback
from telegram.ext import CallbackQueryHandler, PicklePersistence, Updater, CommandHandler,\
	MessageHandler, Filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup,\
	KeyboardButton, ReplyKeyboardMarkup
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
from json import load
import phonenumbers
from validate_email import validate_email
from sys import exit


s3_queue = queue.Queue()
sheets_queue = queue.Queue()
data_queue = queue.Queue()
incidents_queue = queue.Queue()
locations_queue = queue.Queue()
confirmations_queue = queue.Queue()
s3_client = boto3.client('s3')
confirmation_numbers = set(range(100, 1000000))


def upload_file(file_name):
	s3_queue.put(file_name)
	print("pushed", file_name)
	return True


my_persistence = PicklePersistence(filename='database.noext')
admin_gspread_link = "https://docs.google.com/spreadsheets/d/1lRuxnrYkb6_QHvdEPHyLeFea3kIvF6-lPOMTOXsJljo/edit?usp=sharing"
with open("client_secret.json", encoding="utf-8") as f:
	s = load(f)
	GSPREAD_ACCOUNT_EMAIL = s["client_email"]
gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', ['https://spreadsheets.google.com/feeds']))
sh = gc.open_by_url(admin_gspread_link)
name = "LOGS"
try:
	worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	worksheet = sh.worksheet(name)
	worksheet.insert_row(["Дата и время", "ID пользователя", "Событие", "Участок"], 1)
name = "USERDATA"
try:
	data_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	data_worksheet = sh.worksheet(name)
	data_worksheet.insert_row(["ID пользователя", "ФИО", "Номер телефона", "E-Mail", "Серия и номер паспорта", "Город", "Участок"], 1)
name = "INCIDENTS"
try:
	incidents_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	incidents_worksheet = sh.worksheet(name)
	incidents_worksheet.insert_row(["Дата и время", "ID пользователя", "Участок", "Город", "Ссылка", "Тип вложения"], 1)
name = "LOCATIONS"
try:
	locations_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	locations_worksheet = sh.worksheet(name)
	locations_worksheet.insert_row(["ID пользователя", "Широта", "Долгота", "Место", "Город"], 1)
name = "CONFIRMATIONS"
try:
	confirmations_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	confirmations_worksheet = sh.worksheet(name)
	confirmations_worksheet.insert_row(["ID пользователя", "Дата и время", "Файл", "Номер подтверждения", "Место", "Город"], 1)


def push_s3_job():
	print("gotta push s3")
	while not s3_queue.empty():
		print("getting...")
		file_name = s3_queue.get()
		print(file_name)
		try:
			response = s3_client.upload_file(file_name, "statpad-logs", file_name, ExtraArgs={'ACL':'public-read'})
			print("success", file_name)
		except ClientError as e:
			print(e)
			print("nope, pushing again", file_name)
			s3_queue.put(file_name)
	print("done, s3 empty")


def push_sheets_job():
	print("gotta push sheets")
	while not sheets_queue.empty():
		print("getting... sheets")
		row = sheets_queue.get()
		print("got", row)
		try:
			worksheet.insert_row(row, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			sheets_queue.put(row)
	print("done, sheets empty")


def push_data_job():
	print("gotta push data")
	while not data_queue.empty():
		print("getting... data")
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


def push_incidents_job():
	print("gotta push incidents")
	while not incidents_queue.empty():
		print("getting... incidents")
		row = incidents_queue.get()
		print("got", row)
		try:
			incidents_worksheet.insert_row(row, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			data_queue.put(row)
	print("done, incidents empty")


def push_locations_job():
	print("gotta push locations")
	while not locations_queue.empty():
		print("getting... locations")
		row = locations_queue.get()
		print("got", row)
		try:
			locations_worksheet.insert_row(row, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			locations_queue.put(row)
	print("done, locations empty")


def push_confirmations_job():
	print("gotta push confirmations")
	while not confirmations_queue.empty():
		print("getting... confirmations")
		row = confirmations_queue.get()
		print("got", row)
		try:
			confirmations_worksheet.insert_row(row, 2)
			print("success")
		except Exception as e:
			print(e)
			print("failed, pushing back", row)
			confirmations_queue.put(row)
	print("done, confirmations empty")


def send_location(uid, longitude, latitude, place, city):
	locations_queue.put([uid, longitude, latitude, place, city])


def send_confirmation(uid, file_link, confirmation_number, place, city):
	confirmations_queue.put([uid, str(datetime.now()), file_link, confirmation_number, place, city])


def send_data(uid, event_type, data, place, city):
	res = []
	incident_res = []
	if event_type == "name":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: ФИО {data}", ""]
	if event_type == "phone_number":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: Номер телефона {data}", ""]
	if event_type == "email":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: Email {data}", ""]
	if event_type == "passport_info":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: Паспорт {data}", ""]
	if event_type == "city":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: Город {data}", ""]
	if event_type == "place":
		res = [str(datetime.now()), uid, f"Пользователь ввёл данные: Участок {data}", place]
	if event_type == "inc":
		if data == "m":
			res = [str(datetime.now()), uid, "М+", place]
		elif data == "f":
			res = [str(datetime.now()), uid, "Ж+", place]
	elif event_type == "dec":
		if data == "m":
			res = [str(datetime.now()), uid, "М-", place]
		elif data == "f":
			res = [str(datetime.now()), uid, "Ж-", place]
	elif event_type == "incident_started":
		res = [str(datetime.now()), uid, f"Инициировано событие {data}", place]
	elif event_type == "incident_ended":
		res = [str(datetime.now()), uid, f"Завершено событие {data}", place]
	elif event_type == "incident_data":
		if data["type"] == "text":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} добавлен текст: {data['text']}", place]
		elif data["type"] == "document":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} прикреплён документ: {data['file_link']}", place]
			incident_res = [str(datetime.now()), uid, place, city, data['file_link'], "Документ"]
		elif data["type"] == "photo":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} прикреплена фотография: {data['file_link']}", place]
			incident_res = [str(datetime.now()), uid, place, city, data['file_link'], "Фотография"]
		elif data["type"] == "video":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} прикреплено видео: {data['file_link']}", place]
			incident_res = [str(datetime.now()), uid, place, city, data['file_link'], "Видео"]
		elif data["type"] == "voice":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} прикреплено голосовое сообщение: {data['file_link']}", place]
			incident_res = [str(datetime.now()), uid, place, city, data['file_link'], "Голосовое сообщение"]
		elif data["type"] == "video_note":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} прикреплена видеозаметка: {data['file_link']}", place]
			incident_res = [str(datetime.now()), uid, place, city, data['file_link'], "Видеозаметка"]
		elif data["type"] == "caption":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} добавлена подпись для вложений: {data['text']}", place]
		elif data["type"] == "location":
			res = [str(datetime.now()), uid, f"К событию {data['incident_id']} добавлена геолокация: {data['longitude']} {data['latitude']}", place]
	if res:
		print("pushing res to queue", res)
		sheets_queue.put(res)
		print("pushed")
	if incident_res:
		print("pushing incident_res to queue", res)
		incidents_queue.put(incident_res)
		print("pushed")


def start(update, context):
	context.user_data["status"] = "name"
	if "users" not in context.bot_data:
		context.bot_data["users"] = {}
	context.bot_data["users"][str(update.message.chat_id)] = {"last_updated_location": datetime.now() - timedelta(hours=1)}
	update.message.reply_text('Введите, пожалуйста, ФИО')


def button(update, context):
	context.user_data["city"] = update.callback_query.data
	context.user_data["status"] = "place"
	update.callback_query.edit_message_text('Введите, пожалуйста, участок')


def texter(update, context):
	uid = str(update.message.chat_id)
	status = context.user_data["status"]
	print(status)
	if status in ["name", "phone_number", "email", "passport_info", "place"]:
		if status == "name":
			name = update.message.text.split()
			if len(name) != 3 or not all(i.isalpha() for i in name):
				update.message.reply_text('Неверный формат ФИО, попробуйте ещё раз')
			else:
				context.user_data["name"] = update.message.text
				context.user_data["status"] = "phone_number"
				update.message.reply_text('Введите, пожалуйста, номер телефона')
		if status == "phone_number":
			try:
				phone_number = phonenumbers.parse(update.message.text, None)
				if not phonenumbers.is_valid_number(phone_number):
					update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
				else:
					context.user_data["phone_number"] = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
					context.user_data["status"] = "email"
					update.message.reply_text('Введите, пожалуйста, Ваш email')
			except Exception as e:
				print(e)
				try:
					phone_number = phonenumbers.parse(update.message.text, "RU")
					if not phonenumbers.is_valid_number(phone_number):
						update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
					else:
						context.user_data["phone_number"] = phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
						context.user_data["status"] = "email"
						update.message.reply_text('Введите, пожалуйста, Ваш email')
				except Exception as e:
					print(e)
					update.message.reply_text('Неверный формат номера, попробуйте ещё раз')
		if status == "email":
			if not validate_email(update.message.text, check_mx=True):
				update.message.reply_text('Неверный email, попробуйте ещё раз')
			else:
				context.user_data["email"] = update.message.text
				context.user_data["status"] = "passport_info"
				update.message.reply_text('Введите, пожалуйста, серию и номер паспорта')
		if status == "passport_info":
			context.user_data["passport_info"] = update.message.text
			context.user_data["status"] = "city"
			update.message.reply_text('Введите, пожалуйста, город', reply_markup=InlineKeyboardMarkup([
				[InlineKeyboardButton(city, callback_data=city)] for city in context.bot_data["cities"]
			]), one_time_keyboard=True)
		if status == "place":
			context.user_data["place"] = update.message.text
			context.user_data["status"] = "ready"
			data_queue.put([uid, context.user_data["name"], context.user_data["phone_number"], context.user_data["email"], context.user_data["passport_info"], context.user_data["city"], context.user_data["place"]])
			update.message.reply_text('Регистрация успешно пройдена. Спасибо!', reply_markup=ReplyKeyboardMarkup([
				[KeyboardButton("М+"), KeyboardButton("Ж+")],
				[KeyboardButton("Создать инцидент")],
				[KeyboardButton("М-"), KeyboardButton("Ж-")],
				[KeyboardButton("Подтвердить голос")]
			]), one_time_keyboard=True)
		send_data(uid=uid, event_type=status, data=update.message.text, place=context.user_data["place"] if "place" in context.user_data else "", city=context.user_data["city"] if "city" in context.user_data else "")
	elif status == "admin":
		text = update.message.text.split('\n')
		context.user_data["status"] = "ready"
		context.bot_data["cities"] = text
		update.message.reply_text("Список городов обновлён /menu")
	else:
		location_updated = False
		if status == "awaiting_location" and update.message.location:
			location = update.message.location
			if location:
				send_location(uid=uid, longitude=location.longitude, latitude=location.latitude, place=context.user_data["place"], city=context.user_data["city"])
				location_updated = True
				context.bot_data["users"][uid]["last_updated_location"] = datetime.now()
				context.user_data["status"] = "ready"
				update.message.reply_text('Геолокация обновлена')
		if not location_updated and (datetime.now() - context.bot_data["users"][uid]["last_updated_location"]).seconds >= 1800:
			context.user_data["status"] = "awaiting_location"
			update.message.reply_text("Пришлите, пожалуйста, текущую геолокацию")
		else:
			if status == "ready":
				text = update.message.text
				if text == "М+":
					send_data(uid=uid, event_type="inc", data="m", place=context.user_data["place"], city=context.user_data["city"])
					update.message.reply_text("Счётчик успешно обновлён")
				if text == "Ж+":
					send_data(uid=uid, event_type="inc", data="f", place=context.user_data["place"], city=context.user_data["city"])
					update.message.reply_text("Счётчик успешно обновлён")
				if text == "М-":
					send_data(uid=uid, event_type="dec", data="m", place=context.user_data["place"], city=context.user_data["city"])
					update.message.reply_text("Счётчик успешно обновлён")
				if text == "Ж-":
					send_data(uid=uid, event_type="dec", data="f", place=context.user_data["place"], city=context.user_data["city"])
					update.message.reply_text("Счётчик успешно обновлён")
				if text == "Создать инцидент":
					context.user_data["status"] = "create_incident"
					context.user_data["incident_id"] = ''.join(random.choice(string.ascii_lowercase) for i in range(15))
					send_data(uid=uid, event_type="incident_started", data=context.user_data["incident_id"], place=context.user_data["place"], city=context.user_data["city"])
					update.message.reply_text('Событие инициировано, можете добавлять текст, фото, видео', reply_markup=ReplyKeyboardMarkup([
						[KeyboardButton("Завершить " + context.user_data["incident_id"])]
					]), one_time_keyboard=True)
				if text == "Подтвердить голос":
					context.user_data["status"] = "confirm_vote"
					context.user_data["confirmation_number"] = confirmation_numbers.pop()
					update.message.reply_text(f'Пришлите, пожалуйста, фотографию заполненного Вами бюллетеня. Ваш код подтвержения: {context.user_data["confirmation_number"]}', reply_markup=ReplyKeyboardMarkup([
						[KeyboardButton(f'Отменить подтверждение {context.user_data["confirmation_number"]}')]
					]), one_time_keyboard=True)
			elif status == "confirm_vote":
				text = ''
				try:
					text = update.message.text
					if text == f'Отменить подтверждение {context.user_data["confirmation_number"]}':
						context.user_data["status"] = "ready"
						context.user_data["confirmation_number"] = -1
						update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup([
							[KeyboardButton("М+"), KeyboardButton("Ж+")],
							[KeyboardButton("Создать инцидент")],
							[KeyboardButton("М-"), KeyboardButton("Ж-")],
							[KeyboardButton("Подтвердить голос")]
						]), one_time_keyboard=True)
				except Exception as e:
					print(e)
				if not text:
					try:
						photo = update.message.photo[-1]
						if photo:
							filename = f"{uid}-{context.user_data['confirmation_number']}-{photo.file_id}.jpg"
							photo.get_file().download(filename)
							upload_file(filename)
							send_confirmation(uid=uid, file_link=f"https://statpad-logs.s3.amazonaws.com/{filename}",
											  confirmation_number=context.user_data["confirmation_number"],
											  place=context.user_data["place"], city=context.user_data["city"])
							context.user_data["status"] = "ready"
							update.message.reply_text('Подтверждение отправлено, спасибо!', reply_markup=ReplyKeyboardMarkup([
								[KeyboardButton("М+"), KeyboardButton("Ж+")],
								[KeyboardButton("Создать инцидент")],
								[KeyboardButton("М-"), KeyboardButton("Ж-")],
								[KeyboardButton("Подтвердить голос")]
							]), one_time_keyboard=True)
					except Exception:
						pass
			elif status == "create_incident":
				incident_id = context.user_data["incident_id"]
				message = update.message
				try:
					text = message.text
					if text:
						if text == "Завершить " + incident_id:
							context.user_data["status"] = "ready"
							send_data(uid=uid, event_type="incident_ended", data=context.user_data["incident_id"], place=context.user_data["place"], city=context.user_data["city"])
							update.message.reply_text('Событие успешно завершено', reply_markup=ReplyKeyboardMarkup([
								[KeyboardButton("М+"), KeyboardButton("Ж+")],
								[KeyboardButton("Создать инцидент")],
								[KeyboardButton("М-"), KeyboardButton("Ж-")],
								[KeyboardButton("Подтвердить голос")]
							]), one_time_keyboard=True)
						else:
							send_data(uid=uid, event_type="incident_data", data={
								"incident_id": incident_id,
								"type": "text",
								"text": text
							}, place=context.user_data["place"], city=context.user_data["city"])
							update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					document = message.document
					if document:
						filename = f"{uid}-{incident_id}-{document.file_name}"
						document.get_file().download(filename)
						upload_file(filename)
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "document",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}"
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
						'''
						context.user_data[context.user_data["incident_id"]].append({
							"type": "document",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}",
							"datetime": str(datetime.now())
						})
						'''
				except Exception:
					pass
				try:
					photo = update.message.photo[-1]
					if photo:
						filename = f"{uid}-{incident_id}-{photo.file_id}.jpg"
						photo.get_file().download(filename)
						upload_file(filename)
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "photo",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}"
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					video = message.video
					if video:
						filename = f"{uid}-{incident_id}-{video.file_id}.MOV"
						video.get_file().download(filename)
						upload_file(filename)
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "video",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}"
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					voice = message.voice
					if voice:
						filename = f"{uid}-{incident_id}-{voice.file_id}.oga"
						voice.get_file().download(filename)
						upload_file(filename)
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "voice",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}"
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					video_note = message.video_note
					if video_note:
						filename = f"{uid}-{incident_id}-{video_note.file_id}.mp4"
						video_note.get_file().download(filename)
						upload_file(filename)
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "video_note",
							"file_link": f"https://statpad-logs.s3.amazonaws.com/{filename}"
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					caption = message.caption
					if caption:
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "caption",
							"text": caption
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass
				try:
					location = message.location
					if location:
						send_data(uid=uid, event_type="incident_data", data={
							"incident_id": incident_id,
							"type": "location",
							"longitude": location.longitude,
							"latitude": location.latitude
						}, place=context.user_data["place"], city=context.user_data["city"])
						update.message.reply_text('Вложение принято')
				except Exception:
					pass


def stop(update, context):
	if str(update.message.chat_id) == "814961422":
		os.kill(os.getpid(), signal.SIGINT)
		exit()


def admin(update, context):
	uid = str(update.message.chat_id)
	print(uid)
	if uid in ["814961422", "106052"]:
		context.user_data["status"] = "admin"
		s = '\n'.join(context.bot_data["cities"]) if "cities" in context.bot_data else ""
		update.message.reply_text('Вот список городов:')
		update.message.reply_text(s if s else "Городов пока нет")
		update.message.reply_text('''
			/menu - вернуться в меню
			Или отправьте новый список городов, по одному в каждой строке
		''')


def menu(update, context):
	if "status" not in context.user_data or context.user_data["status"] not in ["ready", "admin", "create_incident"]:
		update.message.reply_text('Вы не закончили регистрацию, нажмите /start')
	else:
		context.user_data["status"] = "ready"
		update.message.reply_text('Главное меню', reply_markup=ReplyKeyboardMarkup([
			[KeyboardButton("М+"), KeyboardButton("Ж+")],
			[KeyboardButton("Создать инцидент")],
			[KeyboardButton("М-"), KeyboardButton("Ж-")],
			[KeyboardButton("Подтвердить голос")]
		]), one_time_keyboard=True)


def main():
	updater = Updater("1361720678:AAFxLegEiYeIf2wOCusSPD4uN7caA6InDC4", use_context=True, persistence=my_persistence)
	dp = updater.dispatcher
	dp.add_handler(CommandHandler("start", start))
	dp.add_handler(CommandHandler("shutdown", stop))
	dp.add_handler(CommandHandler("admin", admin))
	dp.add_handler(CommandHandler("menu", menu))
	dp.add_handler(CallbackQueryHandler(button))
	dp.add_handler(MessageHandler(Filters.all, texter))
	updater.start_polling()
	schedule.every().minute.do(push_s3_job).run()
	schedule.every().minute.do(push_sheets_job).run()
	schedule.every().minute.do(push_data_job).run()
	schedule.every().minute.do(push_locations_job).run()
	schedule.every().minute.do(push_incidents_job).run()
	schedule.every().minute.do(push_confirmations_job).run()
	while True:
		try:
			print(datetime.now())
			for i in schedule.jobs:
				try:
					if i.should_run:
						i.run()
				except Exception as e:
					traceback.print_exc()
			sleep(5)
		except Exception as e:
			print(e)


if __name__ == '__main__':
	main()
