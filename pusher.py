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
sheets_queue = queue.Queue()
data_queue = queue.Queue()
incidents_queue = queue.Queue()
locations_queue = queue.Queue()
confirmations_queue = queue.Queue()
s3_client = boto3.client('s3')


with open("job_data.json", "r", encoding="utf-8") as f:
	s = load(f)
	s3_queue.queue = queue.deque(s["s3_queue"])
	sheets_queue.queue = queue.deque(s["sheets_queue"])
	data_queue.queue = queue.deque(s["data_queue"])
	incidents_queue.queue = queue.deque(s["incidents_queue"])
	locations_queue.queue = queue.deque(s["locations_queue"])
	confirmations_queue.queue = queue.deque(s["confirmations_queue"])


JOBS_ALLOWED = True


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
	data_worksheet.insert_row(["ID пользователя", "Дата регистрации", "ФИО", "Номер телефона", "Серия и номер паспорта", "Город", "Участок"], 1)
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
	locations_worksheet.insert_row(["ID пользователя", "Дата и время", "Широта", "Долгота", "Место", "Город"], 1)
name = "CONFIRMATIONS"
try:
	confirmations_worksheet = sh.worksheet(name)
except Exception as e:
	sh.add_worksheet(title=name, rows="5", cols="20")
	confirmations_worksheet = sh.worksheet(name)
	confirmations_worksheet.insert_row(["ID пользователя", "Дата и время", "Файл", "Номер подтверждения", "Место", "Город"], 1)


def push_s3_job():
	print("gotta push s3")
	while JOBS_ALLOWED and not s3_queue.empty():
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


def push_sheets_job():
	print("gotta push sheets")
	while JOBS_ALLOWED and not sheets_queue.empty():
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
	while JOBS_ALLOWED and not data_queue.empty():
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
	while JOBS_ALLOWED and not incidents_queue.empty():
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
	while JOBS_ALLOWED and not locations_queue.empty():
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
	while JOBS_ALLOWED and not confirmations_queue.empty():
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


push_s3_job()
push_sheets_job()
push_data_job()
push_locations_job()
push_incidents_job()
push_confirmations_job()
