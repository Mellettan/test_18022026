"""
Основная логика для сравнения и синхронизации кластеров PostgreSQL.

Этот модуль содержит функции для вычисления различий между схемами баз данных,
интерактивного взаимодействия с пользователем для применения изменений схемы
и синхронизации данных между тестовой и продуктовой базами данных.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from loguru import logger

from .config import SyncConfig
from .database import ColumnSchema, PostgresInspector, SchemaSnapshot, TableSchema


@dataclass
class SchemaDiff:
    """
    Представляет различия между двумя схемами баз данных.

    Атрибуты:
        new_tables (list[TableSchema]): Список таблиц, присутствующих в тестовой схеме,
                                        но отсутствующих в продуктовой.
        missing_tables (list[TableSchema]): Список таблиц, присутствующих в продуктовой схеме,
                                            но отсутствующих в тестовой (потенциальные "лишние" таблицы).
        orphan_columns (dict[str, list[str]]): Словарь, где ключ — имя таблицы, а значение —
                                                список имен колонок, присутствующих в продуктовой таблице,
                                                но отсутствующих в тестовой (потенциальные "лишние" колонки).
        missing_columns (dict[str, list[ColumnSchema]]): Словарь, где ключ — имя таблицы, а значение —
                                                         список объектов `ColumnSchema`,
                                                         присутствующих в тестовой таблице,
                                                         но отсутствующих в продуктовой.
    """

    new_tables: list[TableSchema]
    missing_tables: list[TableSchema]
    orphan_columns: dict[str, list[str]]
    missing_columns: dict[str, list[ColumnSchema]]


def _compute_diff(
    test_schema: SchemaSnapshot, prod_schema: SchemaSnapshot
) -> SchemaDiff:
    """
    Вычисляет различия между тестовой и продуктовой схемами баз данных.

    Сравнивает таблицы и колонки, выявляя отсутствующие или "лишние" элементы.

    Аргументы:
        test_schema (SchemaSnapshot): Снимок схемы тестовой базы данных.
        prod_schema (SchemaSnapshot): Снимок схемы продуктовой базы данных.

    Возвращает:
        SchemaDiff: Объект, содержащий все выявленные различия схемы.
    """
    logger.debug("Вычисление различий схемы между тестовой и продуктовой базами.")
    new_tables: list[TableSchema] = []
    missing_tables: list[TableSchema] = []
    orphan_columns: dict[str, list[str]] = {}
    missing_columns: dict[str, list[ColumnSchema]] = {}

    # Проверяем таблицы в тестовой схеме на наличие в продуктовой.
    for table in test_schema.tables.values():
        if table.name not in prod_schema.tables:
            logger.info("Обнаружена новая таблица в тестовой схеме", table=table.name)
            new_tables.append(table)
            continue
        prod_table = prod_schema.tables[table.name]
        prod_col_names = {col.name for col in prod_table.columns}
        test_col_names = {col.name for col in table.columns}

        # Ищем "лишние" колонки в продуктовой таблице.
        extra = prod_col_names - test_col_names
        if extra:
            logger.info(
                "Обнаружены лишние колонки в продуктовой таблице",
                table=table.name,
                columns=sorted(extra),
            )
            orphan_columns[table.name] = sorted(extra)
        # Ищем отсутствующие колонки в продуктовой таблице.
        missing = [col for col in table.columns if col.name not in prod_col_names]
        if missing:
            logger.info(
                "Обнаружены отсутствующие колонки в продуктовой таблице",
                table=table.name,
                columns=[c.name for c in missing],
            )
            missing_columns.setdefault(table.name, []).extend(missing)

    # Проверяем таблицы в продуктовой схеме на наличие в тестовой.
    for table in prod_schema.tables.values():
        if table.name not in test_schema.tables:
            logger.info(
                "Обнаружена таблица только в продуктовой схеме", table=table.name
            )
            missing_tables.append(table)

    logger.debug(
        "Различия схемы вычислены.",
        diff=SchemaDiff(new_tables, missing_tables, orphan_columns, missing_columns),
    )
    return SchemaDiff(new_tables, missing_tables, orphan_columns, missing_columns)


def _prompt_discard(objects: Iterable[str]) -> bool:
    """
    Запрашивает у пользователя подтверждение на удаление объектов.

    Аргументы:
        objects (Iterable[str]): Итерируемый объект с именами объектов для удаления.

    Возвращает:
        bool: True, если пользователь подтвердил удаление, False в противном случае.
    """
    choice = (
        input(f"Удалить {', '.join(objects)} из продуктовой базы? [y/N]: ")
        .strip()
        .lower()
    )
    return choice == "y"


def _select_sync_column(
    table_name: str,
    test_table: TableSchema,
    prod_table: TableSchema,
    test_inspector: PostgresInspector,
    prod_inspector: PostgresInspector,
) -> tuple[str, ...] | None:
    """
    Интерактивно предлагает пользователю выбрать столбец (или комбинацию столбцов)
    для синхронизации данных между тестовой и продуктовой таблицами.

    Приоритет отдается существующим первичным ключам и уникальным колонкам.

    Аргументы:
        table_name (str): Имя таблицы, для которой выбирается ключ синхронизации.
        test_table (TableSchema): Схема таблицы в тестовой базе.
        prod_table (TableSchema): Схема таблицы в продуктовой базе.
        test_inspector (PostgresInspector): Инспектор для тестовой базы данных.
        prod_inspector (PostgresInspector): Инспектор для продуктовой базы данных.

    Возвращает:
        tuple[str, ...] | None: Кортеж имен колонок, выбранных в качестве ключа синхронизации,
                                или None, если подходящий ключ не найден или не выбран.
    """
    logger.debug("Выбор ключа синхронизации для таблицы", table=table_name)
    test_cols = {col.name for col in test_table.columns}
    prod_cols = {col.name for col in prod_table.columns}
    common_cols = sorted(test_cols & prod_cols)

    if not common_cols:
        logger.warning(
            "Нет общих колонок для выбора ключа синхронизации.", table=table_name
        )
        return None

    potential_keys = []
    for col in common_cols:
        # Проверяем уникальность колонки в обеих таблицах, чтобы она могла быть ключом синхронизации.
        if test_inspector.is_column_unique(
            table_name, col
        ) and prod_inspector.is_column_unique(table_name, col):
            potential_keys.append(col)

    if not potential_keys:
        # Если нет уникальных общих колонок, но есть PK в prod, предлагаем его.
        if prod_table.primary_key:
            logger.info(
                "Нет уникальных общих колонок, предлагаю использовать существующий PK prod.",
                table=table_name,
                pk=prod_table.primary_key,
            )
            return prod_table.primary_key
        logger.warning(
            "Не найдено подходящих уникальных колонок или PK для синхронизации.",
            table=table_name,
        )
        return None

    print(f"\nТаблица '{table_name}': выберите столбец для синхронизации (ID):")
    for i, col in enumerate(potential_keys, 1):
        suffix = " (PRIMARY KEY)" if (col,) == prod_table.primary_key else ""
        print(f"{i}. {col}{suffix}")

    if prod_table.primary_key and prod_table.primary_key[0] not in potential_keys:
        print(f"p. Использовать текущий PK: {', '.join(prod_table.primary_key)}")

    while True:
        choice = input("Ваш выбор (номер или 'p'): ").strip().lower()
        if choice == "p" and prod_table.primary_key:
            logger.info(
                "Пользователь выбрал существующий PK prod для синхронизации.",
                table=table_name,
                pk=prod_table.primary_key,
            )
            return prod_table.primary_key
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(potential_keys):
                logger.info(
                    "Пользователь выбрал колонку для синхронизации.",
                    table=table_name,
                    column=potential_keys[idx],
                )
                return (potential_keys[idx],)
        print("Некорректный выбор. Пожалуйста, введите номер или 'p'.")


def _sync_data(
    test_schema: SchemaSnapshot,
    prod_schema: SchemaSnapshot,
    test_inspector: PostgresInspector,
    prod_inspector: PostgresInspector,
) -> None:
    """
    Синхронизирует данные между тестовой и продуктовой базами данных.

    Для каждой таблицы, присутствующей в обеих схемах, запрашивает у пользователя
    ключ синхронизации, затем вставляет новые строки и обновляет существующие.

    Аргументы:
        test_schema (SchemaSnapshot): Снимок схемы тестовой базы данных.
        prod_schema (SchemaSnapshot): Снимок схемы продуктовой базы данных.
        test_inspector (PostgresInspector): Инспектор для тестовой базы данных.
        prod_inspector (PostgresInspector): Инспектор для продуктовой базы данных.
    """
    logger.info("Начало синхронизации данных.")
    inserted = 0
    updated = 0
    for table_name, test_table in test_schema.tables.items():
        prod_table = prod_schema.tables.get(table_name)
        if not prod_table:
            logger.debug(
                "Таблица отсутствует в продуктовой базе, пропускаю синхронизацию данных.",
                table=table_name,
            )
            continue

        sync_key = _select_sync_column(
            table_name, test_table, prod_table, test_inspector, prod_inspector
        )

        if not sync_key:
            logger.warning(
                "Пропускаю синхронизацию данных для таблицы: не найден подходящий ключ синхронизации.",
                table=table_name,
            )
            continue

        logger.info(
            "Используется ключ синхронизации для таблицы",
            table=table_name,
            sync_key=sync_key,
        )

        prod_columns = {col.name for col in prod_table.columns}
        shared_columns = [
            col.name for col in test_table.columns if col.name in prod_columns
        ]
        if not shared_columns:
            logger.warning(
                "Нет общих колонок между тестовой и продуктовой таблицами, пропускаю синхронизацию данных.",
                table=table_name,
            )
            continue

        prod_sync_values = prod_inspector.fetch_primary_key_values(table_name, sync_key)

        # Также получаем значения реального Primary Key в prod, чтобы избежать UniqueViolation
        prod_real_pk = prod_table.primary_key
        prod_real_pk_values = set()
        if prod_real_pk:
            prod_real_pk_values = prod_inspector.fetch_primary_key_values(
                table_name, prod_real_pk
            )

        rows = test_inspector.fetch_rows(table_name, shared_columns)
        missing_rows = []
        existing_rows = []
        for row in rows:
            sync_val = tuple(row[col] for col in sync_key)
            if sync_val in prod_sync_values:
                existing_rows.append(row)
                continue

            # Проверка на конфликт по реальному PK
            if prod_real_pk:
                real_pk_val = tuple(row[col] for col in prod_real_pk)
                if real_pk_val in prod_real_pk_values:
                    logger.warning(
                        "Строка с ключом синхронизации отсутствует в prod, "
                        "но ее первичный ключ уже существует. Пропускаю.",
                        table=table_name,
                        sync_key_value=sync_val,
                        primary_key_value=real_pk_val,
                    )
                    continue

            missing_rows.append(row)

        if missing_rows:
            logger.info(
                "Вставка новых строк.", table=table_name, count=len(missing_rows)
            )
            batch_inserted = prod_inspector.insert_rows(
                table_name, shared_columns, missing_rows
            )
            inserted += batch_inserted

        if existing_rows:
            logger.info(
                "Обновление существующих строк.",
                table=table_name,
                count=len(existing_rows),
            )
            batch_updated = prod_inspector.update_rows(
                table_name, sync_key, shared_columns, existing_rows
            )
            updated += batch_updated

    logger.info(
        "Сводка синхронизации данных", rows_inserted=inserted, rows_updated=updated
    )


def run_sync(config: SyncConfig) -> None:
    """
    Выполняет полный цикл синхронизации схемы и данных между базами данных.

    Включает в себя:
    1. Инициализацию логирования.
    2. Извлечение схем тестовой и продуктовой баз данных.
    3. Вычисление различий схемы.
    4. Интерактивное применение изменений схемы (создание/удаление таблиц/колонок).
    5. Синхронизацию данных между общими таблицами.

    Аргументы:
        config (SyncConfig): Объект конфигурации, содержащий DSN баз данных и уровень логирования.
    """
    logger.add("dbsync.log", rotation="1 MB", level=config.log_level)
    logger.info(
        "Запуск синхронизации схемы и данных.",
        test_dsn=config.test_dsn,
        prod_dsn=config.prod_dsn,
    )

    test_snap = PostgresInspector(config.test_dsn).fetch_schema()
    prod_snap = PostgresInspector(config.prod_dsn).fetch_schema()

    diff = _compute_diff(test_snap, prod_snap)
    logger.debug("Вычисленные различия схемы", diff=diff)

    inspector = PostgresInspector(config.prod_dsn)
    for table in diff.new_tables:
        logger.info(
            "Создание отсутствующей таблицы в продуктовой базе.", table=table.name
        )
        inspector.create_table(table)

    for table_name, cols in diff.missing_columns.items():
        for column in cols:
            logger.info(
                "Добавление отсутствующей колонки в продуктовой таблице.",
                table=table_name,
                column=column.name,
            )
            inspector.add_column(table_name, column)

    for table_name, columns in diff.orphan_columns.items():
        if _prompt_discard(columns):
            for column in columns:
                logger.info(
                    "Удаление лишней колонки из продуктовой таблицы по запросу пользователя.",
                    table=table_name,
                    column=column,
                )
                inspector.drop_column(table_name, column)
        else:
            logger.warning(
                "Лишние колонки сохранены в продуктовой таблице по запросу пользователя.",
                table=table_name,
                columns=columns,
            )

    for table in diff.missing_tables:
        if _prompt_discard([table.name]):
            logger.warning(
                "Удаление таблицы из продуктовой базы по запросу пользователя.",
                table=table.name,
            )
            inspector.drop_table(table.name)
        else:
            logger.warning(
                "Таблица, существующая только в продуктовой базе, сохранена по запросу пользователя.",
                table=table.name,
            )

    # Повторно получаем схему продуктовой базы после применения изменений.
    prod_snap = inspector.fetch_schema()
    test_inspector = PostgresInspector(config.test_dsn)
    _sync_data(test_snap, prod_snap, test_inspector, inspector)

    logger.info("Синхронизация схемы и данных завершена успешно.")


def main() -> None:
    """
    Главная функция для запуска процесса синхронизации.

    Инициализирует конфигурацию из переменных окружения и запускает `run_sync`.
    """
    config = SyncConfig.from_env()
    run_sync(config)
