import copy
import json
import re
import pymysql
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, DateTime
from datetime import datetime, UTC
from sqlalchemy.orm import sessionmaker, declarative_base
from bs4 import BeautifulSoup
import datetime
from pytz import timezone
try:
    # Define the SERVER
    SERVER_URL = "http://10.109.78.17:8080"
    # SERVER_URL = "http://localhost:8080"
    # Define the timezone
    utc_plus_2 = timezone("Etc/GMT-2")
    # Настройки подключения к исходной БД (portal)
    SOURCE_DB_CONFIG = {
        "host": "192.168.50.9",
        "port": 3306,  # Адрес хоста исходной базы данных
        "user": "soe_site",  # Пользователь БД
        "password": "R7nThBnrppHvyR3w",  # Пароль от БД
        "database": "portal",  # Название базы данных
        "charset": "utf8mb4"  # Кодировка для поддержки спецсимволов
    }

    # Настройки подключения к целевой БД serverhost
    TARGET_DB_URL = "mysql+pymysql://user_portal_test:54(Sh*vPr9@10.109.77.57:3306/portal_test"  # Подключение через SQLAlchemy
    # Настройки подключения к целевой БД localhost
    # TARGET_DB_URL = "mysql+pymysql://root:root_password@10.109.33.46:3309/test_db"
    Base = declarative_base()

    # Определение модели целевой таблицы
    class TargetContent(Base):
        __tablename__ = "news"  # Название таблицы в целевой БД

        id = Column(Integer, primary_key=True, autoincrement=True)  # Первичный ключ
        title = Column(String(255))  # Заголовок
        content = Column(Text)  # Содержимое
        isActive = Column(Boolean)  # Активность записи
        views = Column(Integer, default=0)  # Количество просмотров
        createdAt = Column(DateTime, default=lambda: datetime.datetime.now(utc_plus_2))  # Дата создания
        updatedAt = Column(DateTime, default=lambda: datetime.datetime.now(utc_plus_2))  # Дата обновления
        titleImageId = Column(Integer)
        url = Column(String(255))  # Уникальный alias
        parsed = Column(Boolean)  # Активность записи
        date = Column(String(255))

    class TargetFileNews(Base):
        __tablename__ = 'file_news'
        
        id = Column(Integer, primary_key=True, autoincrement=True)  # Первичный ключ
        fileName = Column(String(255))
        fileExtension = Column(String(255))
        fileType = Column(String(255))
        originalName = Column(String(255))
        createdAt = Column(DateTime, default=lambda: datetime.datetime.now(utc_plus_2))  # Дата создания
        url = Column(String(255))
        newsId = Column(Integer)


    # Определение модели для таблицы title_image_news
    class TitleImageNews(Base):
        __tablename__ = "title_image_news"  # Название таблицы в БД

        id = Column(Integer, primary_key=True, autoincrement=True)  # Первичный ключ
        url = Column(String(255), nullable=False)  # URL изображения
        title = Column(String(255), nullable=False)  # Название изображения

    # Подключение к исходной БД
    source_conn = pymysql.connect(**SOURCE_DB_CONFIG)
    cursor = source_conn.cursor(pymysql.cursors.DictCursor)  # Использование курсора с отображением колонок как словаря

    # Подключение к целевой БД через SQLAlchemy
    engine = create_engine(TARGET_DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Подключение к целевой БД для работы с таблицей urls serverhost 
    target_conn = pymysql.connect(
        host="10.109.77.57",
        port=3306,
        user="user_portal_test",
        password="54(Sh*vPr9",
        database="portal_test",
        charset="utf8mb4"
    )
    # Подключение к целевой БД для работы с таблицей urls localhost
    # target_conn = pymysql.connect(
    #     host="10.109.33.46",
    #     port=3309,
    #     user="root",
    #     password="root_password",
    #     database="test_db",
    #     charset="utf8mb4"
    # )
    target_cursor = target_conn.cursor(pymysql.cursors.DictCursor)

    # Получение данных из исходной таблицы
    cursor.execute("SELECT id, title, introtext, state, hits, created, modified, alias FROM pmguy_content WHERE catid = 12")
    rows = cursor.fetchall()  # Извлечение всех записей

    for row in rows:
        # Ensure `updatedAt` has a valid value
        updated_at = row["modified"]
        if not updated_at or updated_at == "0000-00-00 00:00:00":
            updated_at = row["created"] # Use the current timestamp as a fallback
        alias_value = row["alias"]  # Полученный alias из таблицы urls

        # Проверка наличия записи в таблице news с таким же url
        existing_entry = session.query(TargetContent).filter_by(url=alias_value).first()
        if existing_entry:
            print(f"Запись с url '{alias_value}' уже существует. Пропускаем.")
            continue  # Пропускаем текущую запись

            # Обработка introtext
        introtext = row["introtext"]

        # Регулярное выражение для поиска всех <img> тегов
        img_tags = re.findall(r'<img [^>]*src="([^"]+)"', introtext)

        if img_tags:
            
        
            # Обработка первого <img>
            first_img_src = img_tags[0]
            first_img_filename = first_img_src.split("/")[-1]  # Извлечение имени файла
            first_img_without_ext = first_img_filename.split(".")[0]
            new_first_img_src = f"/uploads/titleImageNews/{first_img_filename}"
            
        
            target_cursor.execute("SELECT id, url FROM title_image_news WHERE url = %s", (new_first_img_src,))
            title_image_news_entry = target_cursor.fetchone()  # Получение записи, если она существует
            

            if not title_image_news_entry:
                # Вставка данных в таблицу title_image_news
                title_image_entry = TitleImageNews(url=new_first_img_src,title=first_img_without_ext)
                session.add(title_image_entry)
                session.commit()

                # Получение ID созданной записи
                title_image_id = title_image_entry.id
                # target_cursor.execute(
                #     "INSERT INTO title_image_news (url, title) VALUES (%s, %s)",
                #     (new_first_img_src, first_img_filename)
                # )
                # target_conn.commit()
                # title_image_value = new_first_img_src  # Используемый alias
            else:
                title_image_id = title_image_news_entry["id"]  # Полученный alias из таблицы urls
            # Удаление первого <img> из текста
            # Удаление первого <img> из текста с помощью BeautifulSoup
            soup = BeautifulSoup(introtext, "html.parser")
            first_img_tag = soup.find("img", {"src": first_img_src})
            if first_img_tag:
                first_img_tag.decompose()  # Удаляем первый <img> тег
            intro = copy.copy(introtext)
            # Преобразуем обратно в строку и обновляем introtext
            introtext = str(soup)
            print(first_img_src in introtext)
            if first_img_src in introtext:
                print(f"Предупреждение: первый <img> с src='{first_img_src}' не был удален.")
                print(f"introtext= {introtext}")


            # Обработка остальных <img>
            def replace_img_src(match):
                img_tag = match.group(0)  # Весь <img ...>
                img_src = match.group(1)  # Только src
                if img_src.startswith("http") or img_src.startswith("https"):  # Если src начинается с http или https, не меняем его
                    return img_tag
                img_filename = img_src.split("/")[-1]
                new_src = f"/uploads/news/old/{img_filename}"

                updated_img_tag = re.sub(r'src="[^"]+"', f'src="{new_src}"', img_tag)
                return updated_img_tag

            introtext = re.sub(r'<img [^>]*src="([^"]+)"[^>]*>', replace_img_src, introtext)

            def replace_video_src(match):
                # video_tag = match.group(0)  # Весь <img ...>
                video_src = match.group(1)
                video_filename = video_src.split("/")[-1]  # Получаем имя файла
                # new_src = f"{SERVER_URL}/uploads/news/old/{video_filename}"
                updated_video_tag = f'<source src="/uploads/news/old/{video_filename}" type="video/mp4"/>'
                return updated_video_tag
            
            # Заменяем пути в видео
            introtext = re.sub(r'<source [^>]*src="([^"]+)"[^>]*>', replace_video_src, introtext)

            def replace_pdf_href(match):
                href = match.group(1)  # Извлекаем значение href
                if href.endswith(".pdf"):  # Проверяем, является ли это PDF-файлом
                    filename = href.split("/")[-1]  # Получаем имя файла
                    # new_src = f"{SERVER_URL}/uploads/news/old/{filename}"
                    updated_file_tag = f'<a href="/uploads/news/old/{filename}" target="_blank">'
                    return updated_file_tag
                return match.group(0)  # Если не PDF, возвращаем оригинальный тег без изменений

            # Заменяем href в ссылках на PDF-файлы
            introtext = re.sub(r'<a [^>]*href="([^"]+)"[^>]*>', replace_pdf_href, introtext)

        introTextInJson = json.dumps({"content": introtext}, ensure_ascii=False, indent=4)
        # Создание объекта для вставки в целевую БД
        content_entry = TargetContent(
            id = row["id"], # ID запису
            title=row["title"],  # Запись заголовка
            content=introTextInJson,  # Запись содержимого
            isActive=True if row["state"] == 1 else False,  # Установка активности в зависимости от state
            views=row["hits"],  # Количество просмотров
            createdAt=row["created"],  # Дата создания
            updatedAt=updated_at,
            titleImageId=title_image_id,  # Связь с TitleImageNews
            url=alias_value,  # Привязанный alias
            parsed=True,
            date = updated_at
        )
        session.add(content_entry)  # Добавление записи в сессию

        


        # Обработка остальных <img>
        def add_img_in_bd(match):
            img_tag = match.group(0)  # Весь <img ...>
            img_src = match.group(1)  # Только src
            if img_src.startswith("http") or img_src.startswith("https"):  # Если src начинается с http или https, не меняем его
                return img_tag
            img_filename = img_src.split("/")[-1]
            new_src = f"/uploads/news/old/{img_filename}"
            existing_in_session = session.query(TargetFileNews).filter_by(url=new_src).first()
            updated_img_tag = re.sub(r'src="[^"]+"', f'src="{new_src}"', img_tag)
            if existing_in_session:
                return updated_img_tag
            with session.no_autoflush:
                target_cursor.execute("SELECT url FROM file_news WHERE url = %s", (new_src))
                file_news = target_cursor.fetchone()  # Получение записи, если она существует
                print("file_news",file_news)
                # Подставляем новый src, сохраняя другие атрибуты
                if not file_news:
                    file_entry = TargetFileNews(
                        fileName = img_filename,
                        fileExtension = img_filename.split(".")[-1],
                        originalName = img_filename,
                        fileType = 'image',
                        url = new_src,
                        newsId = row["id"]
                    )
                    session.add(file_entry)  # Добавление записи в сессию
                else:
                    print(f'Dublicate in {alias_value} img = {new_src}')
            return updated_img_tag

        intro = re.sub(r'<img [^>]*src="([^"]+)"[^>]*>', add_img_in_bd, intro)

        def add_video_in_bd(match):
            video_tag = match.group(0)  # Весь <img ...>
            video_src = match.group(1)
            video_filename = video_src.split("/")[-1]  # Получаем имя файла
            new_src = f"/uploads/news/old/{video_filename}"
            updated_video_tag = f'<source src="/uploads/news/old/{video_filename}" type="video/mp4"/>'
            existing_in_session = session.query(TargetFileNews).filter_by(url=new_src).first()
            if existing_in_session:
                return updated_video_tag
            with session.no_autoflush:
                target_cursor.execute("SELECT url FROM file_news WHERE url = %s", (new_src))
                file_news = target_cursor.fetchone()  # Получение записи, если она существует
                if not file_news:
                    file_entry = TargetFileNews(
                        fileName = video_filename,
                        fileExtension = video_filename.split(".")[-1].split("#")[0],
                        originalName = video_filename,
                        fileType = 'video',
                        url = new_src,
                        newsId = row["id"]
                    )
                    session.add(file_entry)  # Добавление записи в сессию
                else:
                    print(f'Dublicate in {alias_value} video = {new_src}')
            return updated_video_tag
        
        # Заменяем пути в видео
        intro = re.sub(r'<source [^>]*src="([^"]+)"[^>]*>', add_video_in_bd, intro)

        def add_pdf_in_bd(match):
            href = match.group(1)  # Извлекаем значение href
            if href.endswith(".pdf"):  # Проверяем, является ли это PDF-файлом
                filename = href.split("/")[-1]  # Получаем имя файла
                new_src = f"/uploads/news/old/{filename}"
                updated_file_tag = f'<a href="/uploads/news/old/{filename}" target="_blank">'
                existing_in_session = session.query(TargetFileNews).filter_by(url=new_src).first()
                if existing_in_session:
                    return updated_file_tag
                with session.no_autoflush:
                    target_cursor.execute("SELECT url FROM file_news WHERE url = %s", (new_src))
                    file_news = target_cursor.fetchone()  # Получение записи, если она существует
                    if not file_news:
                        file_entry = TargetFileNews(
                            fileName = filename,
                            fileExtension = filename.split(".")[-1],
                            originalName = filename,
                            fileType = 'application',
                            url = new_src,
                            newsId = row["id"]
                        )
                        session.add(file_entry)  # Добавление записи в сессию
                    else:
                        print(f'Dublicate in {alias_value} file = {new_src}')
                return updated_file_tag
            return match.group(0)  # Если не PDF, возвращаем оригинальный тег без изменений

        # Заменяем href в ссылках на PDF-файлы
        intro = re.sub(r'<a [^>]*href="([^"]+)"[^>]*>', add_pdf_in_bd, intro)



    session.commit()  # Сохранение всех изменений в целевой БД
    print("Парсинг завершен.")  # Сообщение о завершении
finally:
    # Закрытие соединений
    cursor.close()
    source_conn.close()
    target_cursor.close()
    target_conn.close()
    session.close()
    print("соединение закрыто.")  # Сообщение о завершении
