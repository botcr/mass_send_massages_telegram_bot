import sqlite3
import time
from telebot import types
from loguru import logger
import os
import zipfile
import telebot
import datetime
import threading
from config import super_admins, bot
import subprocess
import sys


thread_lock = False
checksum_send_order_to_users = 0
checksum_announce = 0
checksum_button_kill = 0
checksum_button_return = 0
checksum_change_message = 0

#для получения текущего московского времени
def get_current_msc_time() -> datetime:
    delta = datetime.timedelta(hours=3)
    return datetime.datetime.now(datetime.timezone.utc) + delta

def time_check_every_hours():
  while True:
    msc_time = get_current_msc_time()
    print(msc_time.strftime('%H'))
    #обнуляет отклики на заявки
    if msc_time.strftime('%H') == '03':
      with sqlite3.connect('users.db', timeout=15000) as orders:
                    curs = orders.cursor()
                    curs.execute("""UPDATE orders_answers SET key = 9 WHERE key == 1""")
      backup_txt()
    #выкидывает из групп закрытых заявок через 3 недели
    if msc_time.strftime('%H') == '04':
      with subprocess.Popen([sys.executable, 'userbot_delete_from_groups.py',]):
              pass
    if msc_time.strftime('%H') == '02':
      backup_db()
    time.sleep(3600)

def message_send_queue():
  global thread_lock
  while True:
    time.sleep(1)
    with sqlite3.connect('orders_message.db', timeout=5000) as orders:
      curs = orders.cursor()
      curs.execute("""SELECT * FROM queue ORDER BY number;""")
      data = curs.fetchone()
    if thread_lock == False and data != None:
      with open('log.txt', 'a') as file:
          file.write(f"{data} {get_current_msc_time()}\n")
      if data[1] == 'button_kill':
        print(data)
        args = (data[3], data[2], data[4])
        t1 = threading.Thread(target=button_kill, args=args)
        t1.start()
        thread_lock = True
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            curs.execute("""DELETE from queue WHERE number == ?""", (data[0],))
      elif data[1] == 'button_return':
        print(data)
        args = (data[3], data[2], data[4])
        t2 = threading.Thread(target=button_return, args=args)
        t2.start()
        thread_lock = True
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            curs.execute("""DELETE from queue WHERE number == ?""", (data[0],))
      elif data[1] == 'change_order_text_in_user_message':
        print(data)
        args = (data[3], data[2], data[4])
        t3 = threading.Thread(target=change_order_text_in_user_message, args=args)
        t3.start()
        thread_lock = True
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            curs.execute("""DELETE from queue WHERE number == ?""", (data[0],))
      elif data[1] == 'send_announce_to_users':
        print(data)
        args = (data[3], data[2], data[4], data[5], data[6])
        t3 = threading.Thread(target=send_announce_to_users, args=args)
        t3.start()
        thread_lock = True
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            curs.execute("""DELETE from queue WHERE number == ?""", (data[0],))
      elif data[1] == 'send_order_to_users':
        print(data)
        args = (data[3], data[2], data[4], data[5])
        t3 = threading.Thread(target=send_order_to_users, args=args)
        t3.start()
        thread_lock = True
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            curs.execute("""DELETE from queue WHERE number == ?""", (data[0],))
        
def send_announce_to_users(order_text, order_id, from_user_id, work_location,rating_from):
    global thread_lock
    global checksum_announce
    global time_check
    print(work_location, rating_from)
    try:
        with open('log.txt', 'a') as file:
          file.write(f"Начал отправку объявлений заявка {order_id} {get_current_msc_time()}\n")
        with open('Messages_sended.txt', 'a') as file:
          file.write(f"Начал отправку объявлений заявка {order_id} {get_current_msc_time()}\n")
        t1 = datetime.datetime.now()
        with sqlite3.connect('users.db', timeout=15000) as orders:
          curs = orders.cursor()
          # тянем из бд admins_id
          curs.execute("""SELECT admin_id FROM admins;""")
          admins = curs.fetchall()
          #строим поисковый запрос по кусочкам
          w = "SELECT user_id FROM users WHERE is_ban == ? AND is_active == ?"
          elements = [0, 1]
          if rating_from != None:
            w = w + " AND rating >= ?"
            elements.append(rating_from)
          if work_location != 'Все':
            w = w + " AND work_location == ?"
            elements.append(work_location)
          w = w + ";"
          print(w, elements)
          user1 = curs.execute(w, elements).fetchall()
        admins_list = []
        for admin in admins:
          admins_list.append(admin[0])
        #через while чтобы не терять 1 пользователя попадающего на ошибку
        sa = 0
        checksum_announce = 0
        time_check = datetime.datetime.now()
        for_threads = []
        n = int(len(user1)/10)
        k = 0
        temp = []
        users_number = 0
        for zzz in user1:
          if zzz[0] not in super_admins and zzz[0] not in admins_list:
            temp.append(zzz[0])
            if k == n:
              k = 0
              for_threads.append(temp)
              temp = []
            k+=1
            users_number+=1
        try:
          for_threads.append(temp)
          print('>>>>>', temp)
        except Exception as e:
          print('289', e)
        print(n, len(for_threads))
        for users_id in for_threads:
            print('отдал пользователей', len(users_id))
            # проверка на админа
            t202 = threading.Thread(target=send_announce_tr,
            args=(users_id, order_text, order_id))
            t202.start()
            sa+=1
        while users_number != checksum_announce and (datetime.datetime.now() - time_check).total_seconds() < 30:
          print((datetime.datetime.now() - time_check).total_seconds())
          print(users_number, checksum_announce)
          time.sleep(1)
        t2 = datetime.datetime.now()
        t = t2-t1
        with open('log.txt', 'a') as file:
          file.write(f"Закончил отправку объявлений, было отправлено {checksum_announce} сообщений из {users_number} send_announce_to_users заняла\n{t} / {get_current_msc_time()}\n")
        print('send_announce_to_users закончил')
        bot.send_message(from_user_id, f'Объявление {order_id} разослано {checksum_announce} пользователям из {users_number}')
        if users_number != checksum_announce:
           with open('log.txt', 'a') as file:
              file.write(f"!!! Объявление {order_id} разослано {checksum_announce} пользователям из {users_number} {get_current_msc_time()}\n")
        thread_lock = False
    except Exception:
        logger.exception('-')
        thread_lock = False

def send_announce_tr(users_id, text, order_id):
  for user_id in users_id:
    send_announce_tr_2(user_id, text, order_id)

def send_announce_tr_2(user_id, text, order_id):
  global checksum_announce
  global time_check
  try:
    bot.send_message(user_id, text)
    checksum_announce+=1
    time_check = datetime.datetime.now()
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"send_announce_tr_2 успешно, user_id{user_id}, заявка {order_id} / {get_current_msc_time()}\n")
  except telebot.apihelper.ApiTelegramException as e:
    #429 - слишком много запросов
    if e.error_code == 429:
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_announce_tr_2 user_id{user_id}, заявка {order_id}  {get_current_msc_time()}\n")
      time.sleep(e.result_json['parameters']['retry_after'])
      send_announce_tr_2(user_id, text, order_id)
      
    elif e.error_code == 403:
      with sqlite3.connect('users.db', timeout=15000) as data:
          curs = data.cursor()
          curs.execute("""UPDATE users SET is_active = 0 WHERE user_id == ?""",
                           (user_id,))
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_announce_tr_2 user_id{user_id}, заявка {order_id}  {get_current_msc_time()}\n")
      checksum_announce+=1
      time_check = datetime.datetime.now()
    else:
      print('send_announce_tr_2')
      print(e)
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_announce_tr_2 user_id{user_id}, заявка {order_id}  {get_current_msc_time()}\n")
      checksum_announce+=1
      time_check = datetime.datetime.now()
      


def send_order_to_users(order_text, order_id, from_user_id, work_location):
    global thread_lock
    global checksum_send_order_to_users
    global time_check
    try:
        with open('log.txt', 'a') as file:
          file.write(f"Начал send_order_to_users заявка {order_id} {get_current_msc_time()}\n")
        with open('Messages_sended.txt', 'a') as file:
          file.write(f"Начал send_order_to_users заявка {order_id} {get_current_msc_time()}\n")
        t1 = datetime.datetime.now()
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
            curs = orders.cursor()
            order_id_table = 'order_' + str(order_id)
            # создаем таблицу с названием как ордер id для записи всех сообщений которые были отосланы
            q = """CREATE TABLE IF NOT EXISTS {table_name} (
            chat_id INTEGER,
            message_id INTEGER
            )"""
            curs.execute(q.format(table_name=order_id_table))
        with sqlite3.connect('users.db', timeout=15000) as orders:
            curs = orders.cursor()
            # тянем из бд admins_id
            curs.execute("""SELECT admin_id FROM admins;""")
            admins = curs.fetchall()
            curs.execute("""SELECT who_create FROM orders WHERE order_id == ?;""", (order_id,))
            username2 = curs.fetchone()
            user1 = curs.execute("""SELECT user_id FROM users WHERE is_ban == 0 AND is_active == 1 AND work_location == ?;""", (work_location,)).fetchall()
        admins_list = []
        for admin in admins:
            admins_list.append(admin[0])
        i = 0
        # делаем кнопки для пользователя с чатом того, кто заявку создал
        try:
            username1 = username2[0]
            username = username1[1:]
            url = 'tg://resolve?domain=' + username
            markup = types.InlineKeyboardMarkup()
            but1 = types.InlineKeyboardButton(text='Еду', callback_data='Im coming')
            but2 = types.InlineKeyboardButton(text='Еду с другом', callback_data='Im coming with a friend')
            but3 = types.InlineKeyboardButton(text='Задать вопрос', url=url)
            markup.add(but1, but2, but3)
        except TypeError:
            markup = types.InlineKeyboardMarkup()
            but1 = types.InlineKeyboardButton(text='Еду', callback_data='Im coming')
            but2 = types.InlineKeyboardButton(text='Еду с другом', callback_data='Im coming with a friend')
            markup.add(but1, but2)
        checksum_send_order_to_users = 0
        #через while чтобы не терять 1 пользователя попадающего на ошибку
        for_threads = []
        n = int(len(user1)/10)
        k = 0
        temp = []
        users_number = 0
        for zzz in user1:
          if zzz[0] not in super_admins and zzz[0] not in admins_list:
            temp.append(zzz[0])
            if k == n:
              k = 0
              for_threads.append(temp)
              temp = []
            k+=1
            users_number+=1
        try:
          for_threads.append(temp)
          print('добавл еще тред', len(temp))
        except Exception as e:
          print('289', e)
        print(n, len(for_threads))
        for users_id in for_threads:
            print('отдал пользователей', len(users_id))
            # проверка на админа
            try:
                  args202 = (users_id, order_text, order_id_table, markup, order_id)
                  t202 = threading.Thread(target=send_order_tr,
                  args=args202)
                  t202.start()
                  i+=1
            except Exception:
              i+=1  
              logger.exception('-')
        time_check = datetime.datetime.now()
        while users_number != checksum_send_order_to_users and (datetime.datetime.now() - time_check).total_seconds() < 30:
          print((datetime.datetime.now() - time_check).total_seconds())
          print(users_number, checksum_send_order_to_users)
          time.sleep(1)
        t2 = datetime.datetime.now()
        t = t2-t1
        with open('log.txt', 'a') as file:
          file.write(f"Закончил рассылать заявку {order_id}, было разослано сообщений {checksum_send_order_to_users} из {users_number} созданных тредов функцией send_order_to_users заняла\n{t} / {get_current_msc_time()}\n")
        print('send_order_to_users закончил')
        bot.send_message(from_user_id, f'Заявка {order_id} разослана {checksum_send_order_to_users} из {users_number} пользователям')
        if users_number != checksum_send_order_to_users:
          with open('log.txt', 'a') as file:
              file.write(f"!!! Заявка {order_id} разослана {checksum_send_order_to_users} пользователям из {users_number} {get_current_msc_time()}\n")
        thread_lock = False
    except Exception:
        logger.exception('-')
        thread_lock = False

def send_order_tr(users_id, text, order_id_table, markup, order_id):
  for user_id in users_id:
    send_order_tr_2(user_id, text, order_id_table, markup, order_id)
    
def send_order_tr_2(user_id, text, order_id_table, markup, order_id):
  global checksum_send_order_to_users
  global time_check
  try:
    t10 = datetime.datetime.now()
    msg = bot.send_message(user_id, text, reply_markup=markup)
    t11 = datetime.datetime.now()
    # записываем все сообщения которые были отосланы
    with sqlite3.connect('orders_message.db', timeout=15000) as orders:
        curs = orders.cursor()
        w = """INSERT INTO {table_name} (chat_id, message_id) VALUES (?, ?)"""
        curs.execute(w.format(table_name=order_id_table), (msg.chat.id, msg.message_id))
    t22 = datetime.datetime.now()
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"send_order_tr_2 успешно, user_id {user_id}, заявка {order_id} отсылка сообщения заняла {t11-t10} запись в базу заняла {t22-t11} {get_current_msc_time()}\n")
    checksum_send_order_to_users+=1
    time_check = datetime.datetime.now()
  except telebot.apihelper.ApiTelegramException as e:
    #429 - слишком много запросов
    if e.error_code == 429:
      print(e)
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_order_tr_2 user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      time.sleep(e.result_json['parameters']['retry_after'])
      send_order_tr_2(user_id, text, order_id_table, markup, order_id)
     
    elif e.error_code == 403:
      print(e)
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_order_tr_2 user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      with sqlite3.connect('users.db', timeout=15000) as data:
          curs = data.cursor()
          curs.execute("""UPDATE users SET is_active = 0 WHERE user_id == ?""",
                           (user_id,))
      checksum_send_order_to_users+=1
      time_check = datetime.datetime.now()
    else:
      print('send_order_to_users')
      print(e)
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"{e} send_order_tr_2 user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      checksum_send_order_to_users+=1
      time_check = datetime.datetime.now()


def button_kill(order_text, order_id, from_user_id):
  global thread_lock
  global checksum_button_kill
  global time_check
  try:
            with open('log.txt', 'a') as file:
              file.write(f"Начал заявка {order_id} функция button_kill {get_current_msc_time()}\n")
            with open('Messages_sended.txt', 'a') as file:
              file.write(f"Начал заявка {order_id} функция button_kill {get_current_msc_time()}\n")
            t1 = datetime.datetime.now()
            with sqlite3.connect('orders_message.db', timeout=5000) as orders:
                curs = orders.cursor()
                table_name = 'order_' + str(order_id)
                w = """SELECT chat_id, message_id FROM {table_name};"""
                curs.execute(w.format(table_name=table_name))
                users_messages = curs.fetchall()
            with sqlite3.connect('users.db', timeout=15000) as orders:
                curs = orders.cursor()
                curs.execute("""SELECT user_id FROM orders_answers WHERE order_id == ? AND key != 9;""", (order_id,))
                users_already_answered2 = curs.fetchall()
            users_already_answered = []
            for s in users_already_answered2:
              users_already_answered.append(s[0])
            bk = 0
            checksum_button_kill = 0
            for_threads = []
            n = int(len(users_messages)/10)
            k = 0
            temp = []
            users_number = 0
            for zzz in users_messages:
              if zzz[0] not in users_already_answered:
                temp.append(zzz)
                if k == n:
                  k = 0
                  for_threads.append(temp)
                  temp = []
                k+=1
                users_number+=1
            try:
              for_threads.append(temp)
              print('добавл еще тред', len(temp))
            except Exception as e:
              print('289', e)
            print(n, len(for_threads))
            for users_id in for_threads:
                print('отдал пользователей', len(users_id))
                args203 = (users_id, order_text, order_id)
                t203 = threading.Thread(target=button_kill_tr, args=args203)
                t203.start()
                bk+=1
            time_check = datetime.datetime.now()
            while users_number != checksum_button_kill and (datetime.datetime.now() - time_check).total_seconds() < 30:
              print((datetime.datetime.now() - time_check).total_seconds())
              print(users_number, checksum_button_kill)
              time.sleep(1)
            t2 = datetime.datetime.now()
            t = t2-t1
            with open('log.txt', 'a') as file:
              file.write(f"Закончил закрытие, заявка {order_id}, было закрыто {checksum_button_kill} из {users_number} сообщений функцией button_kill заняла\n{t} / {get_current_msc_time()}\n")
            bot.send_message(from_user_id, f'Заявка {order_id} была успешно закрыта для откликов у {checksum_button_kill} из {users_number} пользователей')
            print('button_kill закончил')
            if users_number != checksum_button_kill:
             with open('log.txt', 'a') as file:
              file.write(f"!!! Заявка {order_id} была закрыта для откликов у {checksum_button_kill} из {users_number} пользователей функция button_kill {get_current_msc_time()}\n")
            thread_lock = False
  except Exception:
    logger.exception('-')
    time.sleep(2)
    thread_lock = False

def button_kill_tr(users, order_text, order_id):
  for user in users:
    button_kill_tr_2(user[0], user[1], order_text, order_id)
  
def button_kill_tr_2(user_id, message_id, order_text, order_id):
  global checksum_button_kill
  global time_check
  try:
    bot.edit_message_text(chat_id=user_id, message_id=message_id,
                                        text='❌ Заявка закрыта!   \n' + order_text, reply_markup=None)
    checksum_button_kill+=1
    time_check = datetime.datetime.now()
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_kill_tr_2 успешно, user_id {user_id}, заявка{order_id} {get_current_msc_time()}\n")
  except telebot.apihelper.ApiTelegramException as e:
    #429 - слишком много запросов
    if e.error_code == 429:
      print(e)
      time.sleep(e.result_json['parameters']['retry_after'])
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_kill_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      button_kill_tr_2(user_id, message_id, order_text, order_id)
     
    elif e.error_code == 403:
      print(e)
      with sqlite3.connect('users.db', timeout=15000) as data:
          curs = data.cursor()
          curs.execute("""UPDATE users SET is_active = 0 WHERE user_id == ?""",
                           (user_id,))
      checksum_button_kill+=1
      time_check = datetime.datetime.now()
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_kill_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
    else:
      checksum_button_kill+=1
      time_check = datetime.datetime.now()
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_kill_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
  except Exception as e:
            print(e)
            with open('Messages_sended.txt', 'a') as file:
                file.write(f"button_kill_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
            checksum_button_kill+=1
            time_check = datetime.datetime.now()

def button_return(order_text, order_id, from_user_id):
  global thread_lock
  global checksum_button_return
  global time_check
  
  try:
        with open('log.txt', 'a') as file:
              file.write(f"Начал открытие, заявка {order_id}, функцией button_return {get_current_msc_time()}\n")
        with open('Messages_sended.txt', 'a') as file:
              file.write(f"Начал открытие, заявка {order_id}, функцией button_return {get_current_msc_time()}\n")
        t1 = datetime.datetime.now()
        table_name = 'order_' + str(order_id)
        with sqlite3.connect('users.db', timeout=15000) as orders:
          curs = orders.cursor()
          curs.execute("""SELECT user_id FROM orders_answers WHERE order_id == ? AND key != 9;""", (order_id,))
          users_already_answered2 = curs.fetchall()
          curs.execute("""SELECT who_create FROM orders WHERE order_id == ?;""", (order_id,))
          username2 = curs.fetchone()
        users_already_answered = []
        with sqlite3.connect('orders_message.db', timeout=5000) as orders:
          curs = orders.cursor()
          w = """SELECT chat_id, message_id FROM {table_name};"""
          curs.execute(w.format(table_name=table_name))
          users_messages = curs.fetchall()
        for s in users_already_answered2:
          users_already_answered.append(s[0])
        br = 0
        checksum_button_return = 0
        username1 = username2[0]
        username = username1[1:]
        url = 'tg://resolve?domain=' + username
        markup = types.InlineKeyboardMarkup()
        but1 = types.InlineKeyboardButton(text='Еду', callback_data='Im coming')
        but2 = types.InlineKeyboardButton(text='Еду с другом', callback_data='Im coming with a friend')
        but3 = types.InlineKeyboardButton(text='Задать вопрос', url=url)
        markup.add(but1, but2, but3)
        for_threads = []
        n = int(len(users_messages)/10)
        k = 0
        temp = []
        users_number = 0
        for zzz in users_messages:
          if zzz[0] not in users_already_answered:
            with sqlite3.connect('users.db', timeout=15000) as orders:
                    curs = orders.cursor()
                    curs.execute("""SELECT is_ban, is_active FROM users WHERE user_id == ?;""", (str(zzz[0]),))
                    is_ban = curs.fetchone()
            if is_ban[0] == 0 and is_ban[1] == 1:
              temp.append(zzz)
              if k == n:
                k = 0
                for_threads.append(temp)
                temp = []
              k+=1
              users_number+=1
        try:
          for_threads.append(temp)
          print('добавл еще тред', len(temp))
        except Exception as e:
          print('289', e)
        print(n, len(for_threads))
        for users_id in for_threads:
          print('отдал пользователей', len(users_id))
          args203 = (users_id, order_text, markup, table_name, order_id)
          t203 = threading.Thread(target=button_return_tr, args=args203)
          t203.start()
          br+=1
        time_check = datetime.datetime.now()
        while users_number != checksum_button_return and (datetime.datetime.now() - time_check).total_seconds() < 30:
              print((datetime.datetime.now() - time_check).total_seconds())
              print(users_number, checksum_button_return)
              time.sleep(1)
        t2 = datetime.datetime.now()
        t = t2-t1
        with open('log.txt', 'a') as file:
          file.write(f"Закончил открытие, заявка {order_id},было открыто {checksum_button_return} из {users_number} сообщений функцией button_return заняла\n{t} / {get_current_msc_time()}\n")
        bot.send_message(from_user_id, f'Заявка {order_id} была успешно открыта для откликов у {checksum_button_return} из {users_number} пользователей')
        print('button_return закончил')
        if users_number != checksum_button_return:
          with open('log.txt', 'a') as file:
              file.write(f"!!! Заявка {order_id} была открыта для откликов у {checksum_button_return} из {users_number} пользователей button_return {get_current_msc_time()}\n")
        thread_lock = False
  except Exception:
        logger.exception('-')
        thread_lock = False


def button_return_tr(users, order_text, markup, table_name, order_id):
  for user in users:
    button_return_tr_2(user[0], user[1], order_text, markup, table_name, order_id)
  
def button_return_tr_2(user_id, message_id, order_text, markup, table_name, order_id):
  global checksum_button_return
  global time_check
  try:
    bot.delete_message(chat_id=user_id, message_id=message_id)
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_return_tr_2 удалил сообщение, user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
    msg = bot.send_message(chat_id=user_id, text='✅ Заявка открыта! \n' + order_text, reply_markup=markup)
    # записываем все сообщения которые были отосланы
    with sqlite3.connect('orders_message.db', timeout=15000) as orders:
        curs = orders.cursor()
        w = """UPDATE {table_name} SET message_id = ? WHERE chat_id == ?"""
        curs.execute(w.format(table_name=table_name), (msg.message_id, msg.chat.id))
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_return_tr_2 отправил новое, user_id {user_id} заявка {order_id} {get_current_msc_time()}\n")
    checksum_button_return+=1
    time_check = datetime.datetime.now()
  except telebot.apihelper.ApiTelegramException as e:
    #429 - слишком много запросов
    if e.error_code == 429:
      print(e)
      time.sleep(e.result_json['parameters']['retry_after'])
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_return_tr ошибка {e} user_id {user_id} заявка {order_id} {get_current_msc_time()}\n")
      button_return_tr_2(user_id, message_id, order_text, markup, table_name, order_id)
      
    elif e.error_code == 403:
      try:
        print(e)
        with sqlite3.connect('users.db', timeout=15000) as data:
          curs = data.cursor()
          curs.execute("""UPDATE users SET is_active = 0 WHERE user_id == ?""",
                           (user_id,))
      except Exception:
         pass
      checksum_button_return+=1
      time_check = datetime.datetime.now()
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_return_tr_2 ошибка {e} user_id {user_id} заявка {order_id} {get_current_msc_time()}\n")
    else:
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"button_return_tr_2 ошибка {e} user_id {user_id} заявка {order_id} {get_current_msc_time()}\n")
      checksum_button_return+=1
      time_check = datetime.datetime.now()


def change_order_text_in_user_message(order_text, order_id, from_user_id):
  global thread_lock
  global checksum_change_message
  global time_check
  try:
        with open('log.txt', 'a') as file:
              file.write(f"Начал менять сообщения, заявка {order_id} функцией change_order_text_in_user_message {get_current_msc_time()}\n")
        with open('Messages_sended.txt', 'a') as file:
              file.write(f"Начал менять сообщения, заявка {order_id} функцией change_order_text_in_user_message {get_current_msc_time()}\n")
        t1 = datetime.datetime.now()
        with sqlite3.connect('users.db', timeout=15000) as orders:
            curs = orders.cursor()
            # меняем текст у пользователей
            table_name = 'order_' + str(order_id)
            curs.execute("""SELECT user_id FROM orders_answers WHERE order_id == ?;""", (order_id,))
            users_already_answered2 = curs.fetchall()
            curs.execute("""SELECT who_create, status FROM orders WHERE order_id == ?;""", (order_id,))
            username2 = curs.fetchone()
        if username2[1] == 1:
          status = 'open'
        else:
          status = 'close'
        users_already_answered = []
        with sqlite3.connect('orders_message.db') as orders:
            curs = orders.cursor()
            w = """SELECT chat_id, message_id FROM {table_name};"""
            curs.execute(w.format(table_name=table_name))
            users_messages = curs.fetchall()
        for s in users_already_answered2:
            users_already_answered.append(s[0])
        checksum_change_message = 0
        username1 = username2[0]
        username = username1[1:]
        url = 'tg://resolve?domain=' + username
        markup = types.InlineKeyboardMarkup()
        but1 = types.InlineKeyboardButton(text='Еду', callback_data='Im coming')
        but2 = types.InlineKeyboardButton(text='Еду с другом', callback_data='Im coming with a friend')
        but3 = types.InlineKeyboardButton(text='Задать вопрос', url=url)
        markup.add(but1, but2, but3)
        for_threads = []
        n = int(len(users_messages)/10)
        k = 0
        temp = []
        users_number = 0
        for zzz in users_messages:
          if zzz[0] not in users_already_answered:
              temp.append(zzz)
              if k == n:
                k = 0
                for_threads.append(temp)
                temp = []
              k+=1
              users_number+=1
        try:
          for_threads.append(temp)
          print('добавил еще тред', len(temp))
        except Exception as e:
          print('289', e)
        print(n, len(for_threads))
        for users in for_threads:
          print('отдал пользователей', len(users))
          args204 = (users, order_text, markup, order_id, status)
          t204 = threading.Thread(target=change_order_text_in_user_message_tr, args=args204)
          t204.start()
        time_check = datetime.datetime.now()
        while users_number != checksum_change_message and (datetime.datetime.now() - time_check).total_seconds() < 30:
              print((datetime.datetime.now() - time_check).total_seconds())
              print(users_number, checksum_change_message)
              time.sleep(1)
        t2 = datetime.datetime.now()
        t = t2-t1
        with open('log.txt', 'a') as file:
              file.write(f"Закончил изменение сообщений, заявка {order_id}, было изменено {checksum_change_message} из {users_number} созданных тредов функцией change_order_text заняла\n{t} / {get_current_msc_time()}\n")
        print('change_order_text закончил')
        bot.send_message(from_user_id, f'Текст заявки изменен у {checksum_change_message} из {users_number} пользователей')
        if users_number != checksum_change_message:
          with open('log.txt', 'a') as file:
              file.write(f"!!!Текст заявки изменен у {checksum_change_message} из {users_number} пользователей {get_current_msc_time()}\n")
        thread_lock = False
  except Exception:
    logger.exception('-')
    thread_lock = False

def change_order_text_in_user_message_tr(users, order_text, markup, order_id, status):
  for user in users:
    change_order_text_in_user_message_tr_2(user[0], user[1], order_text, markup, order_id, status)

def change_order_text_in_user_message_tr_2(user_id, message_id, order_text, markup, order_id, status):
  global checksum_change_message
  global time_check
  try:
    if status == 'open':
      bot.edit_message_text(chat_id=user_id, message_id=message_id,
                                        text='✅ Заявка открыта! \n' + order_text, reply_markup=markup)
    elif status == 'close':
      bot.edit_message_text(chat_id=user_id, message_id=message_id,
                                        text='❌ Заявка закрыта!    \n' + order_text, reply_markup=None)
    checksum_change_message+=1
    time_check = datetime.datetime.now()
    with open('Messages_sended.txt', 'a') as file:
          file.write(f"change_order_text_in_user_message_tr_2 успешно, status {status}, user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
  except telebot.apihelper.ApiTelegramException as e:
    #429 - слишком много запросов
    if e.error_code == 429:
      print(e)
      time.sleep(e.result_json['parameters']['retry_after'])
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"change_order_text_in_user_message_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      change_order_text_in_user_message_tr_2(user_id, message_id, order_text, markup, order_id, status)
    elif e.error_code == 403:
      with sqlite3.connect('users.db', timeout=15000) as data:
          curs = data.cursor()
          curs.execute("""UPDATE users SET is_active = 0 WHERE user_id == ?""",
                           (user_id,))
      checksum_change_message+=1
      time_check = datetime.datetime.now()
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"change_order_text_in_user_message_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
    else:
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"change_order_text_in_user_message_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      checksum_change_message+=1
      time_check = datetime.datetime.now()
  except Exception as e:
      print(e)
      with open('Messages_sended.txt', 'a') as file:
          file.write(f"change_order_text_in_user_message_tr_2 ошибка {e} user_id {user_id}, заявка {order_id} {get_current_msc_time()}\n")
      checksum_change_message+=1
      time_check = datetime.datetime.now()
