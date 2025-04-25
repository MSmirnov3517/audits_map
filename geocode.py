import pandas as pd
import folium
import os
from datetime import datetime, timedelta
import logging
import json
import sys

# Конфигурация из файла
with open('config.json') as config_file:
    config = json.load(config_file)

last_run_str = datetime.now().strftime("%Y-%m-%d %H:%M")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../log.txt', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # Используем sys.stdout с UTF-8
    ],
    encoding='utf-8'  # Указываем кодировку для всех обработчиков
)


def read_excel_data(file_path):
    try:
        df = pd.read_excel(file_path)
        logging.info("Данные успешно прочитаны из файла %s", file_path)
        return df
    except FileNotFoundError:
        logging.error("Файл %s не найден", file_path)
        exit()
    except Exception as e:
        logging.error("Ошибка при чтении файла: %s", e)
        exit()


def validate_columns(df):
    required_columns = {'Широта', 'Долгота'}
    if not required_columns.issubset(df.columns):
        logging.error("В файле Excel отсутствуют необходимые столбцы: %s", required_columns)
        exit()
    logging.info("Все необходимые столбцы присутствуют")


def get_color(status):
    status = str(status).strip().lower()
    if status == "зеленый":
        return 'green'
    elif status == "красный":
        return 'red'
    elif status == "золотой":  # Добавляем обработку золотого статуса
        return 'gold'
    else:
        return 'gray'


def calculate_statistics(df):
    now = datetime.now()
    one_month_ago = now - timedelta(days=30)
    golden_audits = df[df['Статус'].str.strip().str.lower() == "золотой"].shape[0]
    green_audits = df[df['Статус'].str.strip().str.lower() == "зеленый"].shape[0]
    red_audits = df[df['Статус'].str.strip().str.lower() == "красный"].shape[0]
    old_audits = df[(df['Дата аудита'].isna()) | (df['Дата аудита'] < one_month_ago)].shape[0]
    goal_percentage = round(((green_audits + golden_audits) / (green_audits + golden_audits + red_audits)) * 100, 1) if len(df) > 0 else 0

    current_year = datetime.now().year
    without_audits = 0
    for _, row in df.iterrows():
        has_audit_in_2025 = False
        if pd.notna(row['Дата аудита']) and row['Дата аудита'].year == current_year:
            has_audit_in_2025 = True
        else:
            for i in range(7, len(row), 3):
                if i < len(row) and pd.notna(row.iloc[i]) and row.iloc[i].year == current_year:
                    has_audit_in_2025 = True
                    break
        if not has_audit_in_2025:
            without_audits += 1

    return golden_audits, green_audits, red_audits, old_audits, goal_percentage, without_audits


def save_to_excel(df, output_file_path):
    """
    Сохраняет DataFrame в Excel-файл.

    Args:
        df (pandas.DataFrame): DataFrame для сохранения.
        output_file_path (str): Путь к файлу Excel, куда нужно сохранить данные.
    """
    try:
        # Сохраняем DataFrame в Excel
        df.to_excel(output_file_path, index=False, engine='openpyxl')
        logging.info(f"Данные успешно сохранены в файл {output_file_path}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла Excel: {e}")
        raise

def create_map(df, golden_audits, green_audits, red_audits, goal_percentage, old_audits, without_audits):
    m = folium.Map(location=[59.832213, 30.251091], zoom_start=11)

    # Создаем группы для маркеров
    golden_group = folium.FeatureGroup(name='Золотые аудиты', show=True)
    green_group = folium.FeatureGroup(name='Зеленые аудиты', show=True)
    red_group = folium.FeatureGroup(name='Красные аудиты', show=True)
    gray_group = folium.FeatureGroup(name='Серые аудиты', show=True)
    for _, row in df.iterrows():
        lat, lon = row['Широта'], row['Долгота']
        name = row['Название ресторана']
        color = get_color(row['Статус'] if pd.notna(row['Статус']) else "Нет данных")

        popup_text = f"<b>{name}</b>"

        audits = []
        audit_num = 1
        while True:
            if audit_num == 1:
                date_col = 'Дата аудита'
                status_col = 'Статус'
                auditor_col = 'Аудитор'
                report_col = 'Отчет'
            else:
                date_col = f'Дата аудита {audit_num}'
                status_col = f'Статус {audit_num}'
                auditor_col = f'Аудитор {audit_num}'
                report_col = f'Отчет {audit_num}'

            # Проверяем, есть ли все столбцы в DataFrame
            if all(col in df.columns for col in [date_col, status_col, auditor_col, report_col]):
                # Проверяем, есть ли данные хотя бы в одном из столбцов
                if pd.notna(row[date_col]):
                    audits.append((
                        row[date_col],
                        row[status_col] if pd.notna(row[status_col]) else "Нет данных",
                        row[auditor_col] if pd.notna(row[auditor_col]) else None,
                        row[report_col] if pd.notna(row[report_col]) else None
                    ))
                audit_num += 1
            else:
                break




        # Если аудитов нет, добавляем сообщение
        if not audits:
            audits.append((None, "Ещё не было аудита в 2025 году", None, None))

        # Формируем текст popup
        for date, status, auditor, report in audits:
            if pd.notna(date):
                date_str = date.strftime("%d.%m.%Y") if isinstance(date, pd.Timestamp) else str(date)
                popup_text += f"<br><br>Дата аудита: {date_str}"
            if status and status != "Нет данных":
                popup_text += f"<br>Статус: {status}"
            if auditor:
                popup_text += f"<br>Аудитор: {auditor}"
            if report:
                popup_text += f'<br><a href="{report}" target="_blank">Посмотреть отчет</a>'

        # Если аудитов больше одного, обновляем заголовок
        if len(audits) > 1:
            popup_text = f"<b>{name} (Аудитов: {len(audits)})</b>" + popup_text[len(f"<b>{name}</b>"):]

        popup = folium.Popup(popup_text, max_width=300)

        # Создаем маркер
        if name == "Ульянка Санкт-Петербург":
            marker = folium.Marker(
                location=[lat, lon],
                popup=popup,
                icon=folium.Icon(icon='heart', prefix='fa', icon_color=color, className='gold-star')
            )
        elif color == 'gold':
            marker = folium.Marker(
                location=[lat, lon],
                popup=popup,
                icon=folium.Icon(icon='star', prefix='fa', icon_color='gold', className='gold-star')
            )
        else:
            marker = folium.CircleMarker(
                location=[lat, lon],
                radius=8,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                popup=popup
            )

        # Добавляем маркер в соответствующую группу
        if color == 'green':
            marker.add_to(green_group)
        elif color == 'red':
            marker.add_to(red_group)
        elif color == 'gold':
            marker.add_to(golden_group)
        else:
            marker.add_to(gray_group)


    # Добавляем группы на карту
    golden_group.add_to(m)
    green_group.add_to(m)
    red_group.add_to(m)
    gray_group.add_to(m)

    # Добавляем контроль слоев для фильтрации
    folium.LayerControl().add_to(m)

    # Добавление стилизованного блока со статистикой (без изменений)
    stats_html = f'''
    <div class="stats-box">
        <div class="stats-title" onclick="toggleStats()">Статистика аудитов</div>
        <div class="stats-content">
            <div class="stats-item"><span class="gold">⭐</span> Золотые аудиты: <strong>{golden_audits}</strong></div>
            <div class="stats-item"><span class="green">✅</span> Зеленые аудиты: <strong>{green_audits}</strong></div>
            <div class="stats-item"><span class="red">❌</span> Красные аудиты: <strong>{red_audits}</strong></div>
            <div class="stats-item"><span class="goal">📊</span> % в цели: <strong>{goal_percentage}%</strong></div>
            <div class="stats-item"><span class="warning">⏳</span> Без аудита > месяца: <strong>{old_audits}</strong></div>
            <div class="stats-item"><span class="no-audit">🚫</span> Рестораны без аудита в 2025: <strong>{without_audits}</strong></div>
            <div class="stats-item"><span class="info">📅</span> Обновлено: <strong style="font-size: 12px;vertical-align: sub">{last_run_str}</strong></div>
        </div>
    </div>
    <script>
    function toggleStats() {{
        var content = document.querySelector(".stats-content");
        content.style.display = content.style.display === "none" ? "block" : "none";
    }}
    </script>
    '''
    styles = '''
    <style>
    .stats-box {
        z-index: 500;
        position: fixed;
        top: 70px;
        right: 10px;
        width: 250px;
        background-color: white;
        padding: 10px;
        border-radius: 8px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        font-family: Arial, sans-serif;
    }
    .stats-content {
        display: none;
        margin-top: 10px;
    }
    .stats-title {
        font-size: 16px;
        font-weight: bold;
        cursor: pointer;
        padding-bottom: 10px;
        border-bottom: 1px solid #ddd;
    }
    .stats-item {
        margin-bottom: 10px;
        display: flex;
        align-items: center;
    }
    .stats-item strong {
        font-size: 18px;
        margin-left: 5px;
    }
    .green { color: #2ecc71; }
    .red { color: #e74c3c; }
    .gold { color: #f1c40f; }
    .goal { color: #3498db; }
    .warning { color: #f1c40f; }
    .no-audit { color: #7f8c8d; }
    .info { color: #9b59b6; }
    </style>
    '''
    custom_styles = '''
        <style>
        .gold-star {
            color: #ffd700;
            position: relative;
        }
        .gold-star i{
            font-size: 18px;
            position: absolute;
            left: 50%;
            bottom: 0;
            transform: translate(-50%, 0%);
        }
        </style>
    '''
    m.get_root().html.add_child(folium.Element('''
    <style>
    .leaflet-control {
        z-index: 1000 !important;
    }
    </style>
    '''))
    m.get_root().html.add_child(folium.Element("<style>.leaflet-control-attribution {display:none;}</style>"))
    m.get_root().html.add_child(folium.Element(stats_html))
    m.get_root().html.add_child(folium.Element(styles))
    m.get_root().html.add_child(folium.Element(custom_styles))

    return m


def save_map(m, output_path):
    m.save(output_path)
    logging.info("Карта сохранена в %s", output_path)



if __name__ == "__main__":
    file_path = config.get("file_path", "restaurants.xlsx")
    df = read_excel_data(file_path)
    validate_columns(df)
    golden_audits, green_audits, red_audits, old_audits, goal_percentage, without_audits = calculate_statistics(df)
    map_ = create_map(df, golden_audits, green_audits, red_audits, goal_percentage, old_audits, without_audits)
    output_path = config.get("output_path", "index.html")
    save_map(map_, output_path)
    # Сохранение DataFrame в Excel
    excel_output_path = config.get("excel_output_path", "restaurants_output.xlsx")
    save_to_excel(df, excel_output_path)

from git import Repo
import os


def auto_push(file_path, message="update index.html"):
    try:
        logging.info("Попытка выполнить git push...")

        if not os.path.exists(".git"):
            logging.error("[ОШИБКА] Текущая директория не является Git-репозиторием")
            return

        repo = Repo(".")

        # Добавляем явное указание ветки
        branch = repo.active_branch.name

        # Проверяем изменения
        if not repo.index.diff(None) and not repo.untracked_files:
            logging.info("[ИНФО] Нет изменений для коммита")
            return

        repo.index.add([file_path])
        repo.index.commit(message)

        if not repo.remotes:
            logging.error("[ОШИБКА] Не настроен удаленный репозиторий (origin)")
            return

        origin = repo.remote(name='origin')

        # Изменяем push с явным указанием ветки
        push_info = origin.push(refspec=f'{branch}:{branch}')

        # Проверяем результат push
        if any(info.flags & info.ERROR for info in push_info):
            logging.warning("[ПРЕДУПРЕЖДЕНИЕ] Возникли проблемы при отправке изменений")
        else:
            logging.info(f"[УСПЕХ] index.html успешно отправлен в GitHub (ветка {branch})")

    except Exception as e:
        logging.error(f"[ОШИБКА] При работе с Git: {str(e)}", exc_info=True)

auto_push(output_path)