# Архитектура dmp-af: подробное описание

Документ объясняет, как устроен проект **dmp-af** — библиотека, которая компилирует dbt-манифест в набор Airflow DAG.
Отправная точка — функция `compile_dmp_af_dags`.

---

## Содержание

1. [Общая схема потока данных](#1-общая-схема-потока-данных)
2. [Точка входа — `compile_dmp_af_dags`](#2-точка-входа--compile_dmp_af_dags)
3. [Config — главный конфигурационный объект](#3-config--главный-конфигурационный-объект)
4. [Парсеры: DbtNode, DbtSource, Profiles](#4-парсеры-dbtnode-dbtsource-profiles)
5. [DmpAfGraph — граф всего проекта](#5-dmpafgraph--граф-всего-проекта)
6. [DomainDag и его подклассы](#6-domainddag-и-его-подклассы)
7. [DomainDagsRegistry — реестр DAG-ов](#7-domaindagsregistry--реестр-dag-ов)
8. [DagComponent — базовый компонент DAG](#8-dagcomponent--базовый-компонент-dag)
9. [Конкретные компоненты DAG](#9-конкретные-компоненты-dag)
10. [Операторы Airflow](#10-операторы-airflow)
11. [Система расписаний (Scheduling)](#11-система-расписаний-scheduling)
12. [Зависимости между задачами](#12-зависимости-между-задачами)
13. [Функции-сборщики DAG](#13-функции-сборщики-dag)
14. [Интеграции и коллбэки](#14-интеграции-и-коллбэки)
15. [Полная последовательность вызовов](#15-полная-последовательность-вызовов)

---

## 1. Общая схема потока данных

```
manifest.json  ──┐
profiles.yml   ──┼──► compile_dmp_af_dags(manifest_path, config)
dbt_project.yml──┘              │
                                ▼
                       _compile_dbt_dags()
                                │
                     ┌──────────┴──────────┐
                     ▼                     ▼
              DmpAfGraph            dbt_run_model_dag()
           .from_manifest()         (ручной DAG)
                     │
          ┌──────────┴────────────────┐
          ▼                           ▼
   _build_dag_components()   _build_backfill_dag_components()
          │                           │
     DagComponent[]             DagComponent[]
          │
   dbt_main_dags(graph)
          │
    dict[str, DAG]  ──► возвращается в airflow
```

---

## 2. Точка входа — `compile_dmp_af_dags`

**Файл:** [dmp_af/dags.py](dmp_af/dags.py)

```python
def compile_dmp_af_dags(
    manifest_path: str,
    config: Config,
    etl_service_name: Optional[str] = None,
) -> dict[str, DAG]:
```

Это **единственная публичная функция**, которую вызывает пользователь библиотеки в файле `dags.py` Airflow.

### Что делает:
1. Читает `manifest.json` — скомпилированный артефакт dbt, содержащий все узлы (модели, тесты, снапшоты, источники).
2. Читает `profiles.yml` — описание окружений подключения к БД.
3. Читает `dbt_project.yml` — чтобы узнать имя профиля (`profile`).
4. Вызывает `_compile_dbt_dags()` — внутреннюю функцию сборки.

### Параметры:
| Параметр | Тип | Описание |
|---|---|---|
| `manifest_path` | `str` | Путь к `manifest.json` из `dbt compile` |
| `config` | `Config` | Главный конфигурационный объект dmp-af |
| `etl_service_name` | `Optional[str]` | Фильтрует модели по имени ETL-сервиса (папке в проекте). Позволяет в одном dbt-проекте создавать отдельные наборы DAG для разных сервисов |

---

## 3. Config — главный конфигурационный объект

**Файл:** [dmp_af/conf/config.py](dmp_af/conf/config.py)

`Config` — это неизменяемый (`frozen=True`) `attrs`-датакласс, который держит в себе всю конфигурацию. Он передаётся во все компоненты системы.

### Вложенные конфиги:

#### `DbtProjectConfig`
Пути к dbt-проекту:
```python
dbt_project_name: str          # имя проекта (из dbt_project.yml)
dbt_models_path: Path          # путь к папке с моделями
dbt_project_path: Path         # путь к dbt_project.yml
dbt_profiles_path: Path        # путь к profiles.yml
dbt_target_path: Path          # куда складывать скомпилированные файлы
dbt_log_path: Path             # куда писать логи
dbt_schema: str                # схема в БД
additional_dbt_env: dict       # дополнительные env-переменные для dbt
```

#### `DbtDefaultTargetsConfig`
Дефолтные targets (окружения в profiles.yml) для разных типов задач:
```python
default_target: str                  # дефолтный target для всех операторов
default_for_tests_target: str        # target для тестов
default_maintenance_target: str      # target для задач обслуживания
default_backfill_target: str         # target для бэкфилла
```

#### `RetriesConfig`
Политики повторных попыток (retry) для каждого типа задачи. Содержит `RetryPolicy` — число попыток, задержку, экспоненциальный backoff.

#### `DependencyWaitPolicy`
Как строить ожидания зависимостей между доменами:
- `per_domain=True` (дефолт) — одна задача-сенсор на весь upstream-домен. Меньше задач в DAG.
- `per_task=True` — отдельный сенсор на каждую задачу.

#### `CustomAfCallbacksConfig`
Пользовательские callback-функции для DAG и задач:
- `task_on_success_callback`, `task_on_failure_callback`, `task_on_retry_callback`, `task_on_execute_callback`
- `dag_on_failure_callback`, `dag_on_success_callback`, `dag_sla_miss_callback`

#### `MCDIntegrationConfig`
Интеграция с Monte Carlo Data — мониторинг качества данных.

#### `TableauIntegrationConfig`
Интеграция с Tableau для обновления экстрактов после прогона моделей.

#### `K8sConfig`
Настройки Kubernetes-оператора (identity binding для Azure).

---

## 4. Парсеры: DbtNode, DbtSource, Profiles

Эти классы **читают** данные из манифеста dbt и оборачивают их в удобные Python-объекты.

### `DbtNode`
**Файл:** [dmp_af/parser/dbt_node_model.py](dmp_af/parser/dbt_node_model.py)

Представляет один узел из `manifest['nodes']`. Узел может быть:
- **model** — SQL/Python модель
- **snapshot** — снапшот
- **seed** — CSV-файл
- **test** — тест (small/medium/large)

Ключевые свойства и методы:
```python
node.resource_type          # 'model', 'snapshot', 'seed', 'test'
node.resource_name          # имя модели
node.unique_id              # уникальный идентификатор (напр. "model.project.name")
node.domain                 # первая часть fqn — "домен" модели
node.depends_on             # список unique_id зависимостей (других моделей)
node.depends_on_sources     # список unique_id зависимостей (источников)
node.config                 # DbtNodeConfig — конфиг из секции config: в YAML
node.is_model()             # True если это модель
node.is_snapshot()          # True если это снапшот
node.is_large_test()        # True если это большой тест (отдельный DAG)
node.is_medium_test()       # True если это средний тест (после модели, в том же DAG)
node.is_small_test()        # True если это малый тест (встроен в задачу модели)
node.is_at_etl_service(name) # True если узел относится к указанному ETL-сервису
```

#### `DbtNodeConfig`
Конфиг из секции `config:` в YAML-файле модели. Содержит:
- `schedule` — расписание (через `dmp_af.schedule`)
- `dbt_target`, `bf_cluster`, `py_cluster`, `sql_cluster`, `daily_sql_cluster` — выбор target
- `enable_from_dttm`, `disable_from_dttm` — временны́е границы активности модели
- `maintenance` — объект `DmpAfMaintenanceConfig` (TTL, persist_docs, optimize_table и т. д.)
- `dependencies` — словарь `DependencyConfig` для управления зависимостями
- `tableau_refresh` — конфиг обновления Tableau

### `DbtSource`
**Файл:** [dmp_af/parser/dbt_source_model.py](dmp_af/parser/dbt_source_model.py)

Представляет источник данных из `manifest['sources']`. Ключевые свойства:
```python
source.domain               # "домен" источника (из fqn[1])
source.unique_id
source.freshness            # DbtSourceFreshness — критерии свежести
source.external_dag_id      # если задан — ждать этот DAG из meta
source.external_task_id     # задача в external DAG
source.external_schedule    # расписание external DAG
source.need_external_sensor()   # нужен ли внешний сенсор
source.need_to_check_freshness() # нужна ли проверка свежести
```

### `Profiles` / `Profile` / `Target`
**Файл:** [dmp_af/parser/dbt_profiles.py](dmp_af/parser/dbt_profiles.py)

Парсит `profiles.yml`. Особые типы target:
- **`KubernetesTarget`** — запуск задач в Kubernetes Pod (node_pool, image, CPU/memory)
- **`VenvTarget`** — запуск задач в Python virtual environment (requirements, python_version)
- **`Target`** — стандартный SQL target

---

## 5. DmpAfGraph — граф всего проекта

**Файл:** [dmp_af/builder/dmp_af_builder.py](dmp_af/builder/dmp_af_builder.py)

`DmpAfGraph` — центральный объект сборки. Содержит все узлы dbt и строит из них компоненты Airflow DAG.

### Создание через `from_manifest`:
```python
graph = DmpAfGraph.from_manifest(
    manifest,
    profiles,
    project_profile_name,
    config=config,
    etl_service_name=etl_service_name,
)
```

Шаги внутри `from_manifest`:
1. Парсит профиль из `profiles.yml` → объект `Profile`.
2. Итерирует `manifest['nodes']`, создаёт `DbtNode` для каждого.
3. Вызывает `node.set_target_details(profile, config.dbt_default_targets)` — проставляет целевое окружение.
4. Фильтрует по `etl_service_name` если задан.
5. Парсит источники (`manifest['sources']`) → список `DbtSource`.
6. Вызывает `_build_dags()`.

### `_build_dags()`
Оркестрирует всю сборку:
```python
def _build_dags(self):
    dag_components = self._build_dag_components(self.dbt_nodes)    # 1. обычные scheduled-задачи
    self.clear_registries()
    backfill_dag_components = self._build_backfill_dag_components(self.dbt_nodes)  # 2. backfill-задачи
    self.nodes = dag_components + backfill_dag_components
```

### `_build_dag_components(nodes)`
1. `_collect_all_models(nodes)` — создаёт `DagComponent` для каждого узла через `DagComponentFactory`.
2. `_collect_maintenance_components()` — создаёт компоненты обслуживания (TTL, vacuum и т. д.).
3. `_resolve_dependencies(nodes)` — прописывает зависимости между компонентами.
4. `_bind_medium_tests()` — привязывает medium-тесты к моделям.

### Реестры внутри `DmpAfGraph`:
| Реестр | Тип | Назначение |
|---|---|---|
| `_domain_dags_registry` | `DomainDagsRegistry(SCHEDULED)` | Плановые DAG |
| `_domain_bf_dags_registry` | `DomainDagsRegistry(BACKFILL)` | Backfill DAG |
| `_domain_maintenance_dags_registry` | `DomainDagsRegistry(MAINTENANCE)` | Maintenance DAG |
| `_models` | `dict[str, DagModel]` | Все модели по unique_id |
| `_large_tests` | `dict[str, LargeTest]` | Все большие тесты |
| `_dag_components_registry` | `dict[str, DagComponent]` | Все компоненты по unique_id |
| `_medium_tests` | `dict[DomainDag, MediumTests]` | Средние тесты по домену |
| `_maintenance_components` | `dict[DomainDag, dict[...]]` | Компоненты обслуживания |

---

## 6. DomainDag и его подклассы

**Файл:** [dmp_af/builder/domain_dag.py](dmp_af/builder/domain_dag.py)

`DomainDag` — представляет один Airflow DAG. Каждый "домен" в dbt (первый элемент `fqn`) может иметь несколько DAG разных типов.

### Иерархия классов:

```
DomainDag
├── BackfillDomainDag        # DAG для бэкфилла (имя: <domain>__backfill)
├── MaintenanceDomainDag     # DAG для обслуживания таблиц (имя: <domain>__maintenance)
└── LargeTestsDomainDag      # DAG для больших тестов (имя: <domain>__large_tests)
```

### `DomainDag` (базовый класс)

```python
class DomainDag:
    domain_name: str                  # имя домена
    config: Config                    # конфиг
    schedule: BaseScheduleTag         # расписание
    catchup: bool                     # догонять ли пропущенные запуски
    af_dag: DAG | None                # ссылка на Airflow DAG (проставляется позже)

    # Словари зарегистрированных зависимостей от других доменов и сенсоров:
    registered_domains_dependencies: dict[DomainDag, RegistryDomainDependencies]
    registered_source_sensors: dict[str, TaskGroup]
    registered_source_sensor_tasks: dict[tuple, list]
```

Имя DAG: `{domain_name}{schedule}` (например, `payments@daily` → `payments__daily`).

Теги DAG: автоматически добавляются `frontier` (для плановых), `backfill`, `maintenance`, имя домена, имя расписания.

### `BackfillDomainDag`
Создаётся для каждого домена, чтобы давать возможность пересчитать исторические данные.

Особенности:
- Расписание всегда `@daily`.
- Имя: `{domain}__backfill`.
- `wrap_dag_with_endpoints()` — добавляет `BranchPythonOperator` в начало: при первом запуске (scheduled) все задачи пропускаются (`do_nothing`); при ручном перезапуске — задачи выполняются.

### `MaintenanceDomainDag`
Для задач обслуживания таблиц (VACUUM, TTL, persist_docs и т. д.).
- Расписание: `@daily`.
- Имя: `{domain}__maintenance`.
- `catchup=False`.

### `LargeTestsDomainDag`
Для тяжёлых dbt-тестов, которые должны выполняться отдельно.
- Расписание: `@daily`.
- Имя: `{domain}__large_tests`.
- `catchup=False`.

### `DomainDagFactory`
Фабрика — создаёт нужный подкласс `DomainDag` по `DomainDagType`:
```python
DomainDagFactory.create(dag_type, domain_name, schedule, config)
```

### `DomainDagType` (enum)
```python
class DomainDagType(Enum):
    SCHEDULED    = 'scheduled'
    BACKFILL     = 'backfill'
    MAINTENANCE  = 'maintenance'
    LARGE_TESTS  = 'large_tests'
```

---

## 7. DomainDagsRegistry — реестр DAG-ов

**Файл:** [dmp_af/builder/dmp_af_builder.py](dmp_af/builder/dmp_af_builder.py)

`DomainDagsRegistry` — кэш (реестр) объектов `DomainDag`. Для каждого уникального сочетания `(домен, расписание, тип)` возвращает один и тот же объект.

```python
class DomainDagsRegistry:
    def get(self, dbt_node: DbtNode) -> DomainDag:
        ...
```

Хэш ключа зависит от типа реестра:
| Тип | Ключ |
|---|---|
| `SCHEDULED` | `{domain}_{schedule.safe_name}` |
| `BACKFILL` | `{domain}__bf` |
| `MAINTENANCE` | `{domain}__maintenance` |
| `LARGE_TESTS` | `{domain}__large_tests` |

Таким образом, все модели одного домена с одним расписанием попадают в один `DomainDag`.

---

## 8. DagComponent — базовый компонент DAG

**Файл:** [dmp_af/builder/dag_components.py](dmp_af/builder/dag_components.py)

`DagComponent` — абстрактный базовый класс для всего, что может оказаться задачей (или группой задач) в Airflow DAG.

```python
class DagComponent:
    name: str               # имя компонента (resource_name модели)
    domain_dag: DomainDag   # к какому DAG принадлежит
    node_config: DbtNodeConfig

    # Внутренние зависимости:
    _depends_on: set[DagComponent]          # upstream-компоненты
    _depends_on_sources: set[DbtSource]     # upstream-источники
    _domains_dependencies: dict[DomainDag, set[DagComponent]]  # upstream по доменам

    # Airflow-объекты (заполняются в init_af()):
    af_component      # главный оператор или TaskGroup
    model_task        # непосредственно dbt-оператор
    task_group        # TaskGroup если нужен
    af_sensor_endpoint  # точка, которую ждут downstream-сенсоры
```

Ключевые методы:
```python
component.add_dependency(dep)           # добавить upstream-зависимость
component.add_source_dependency(src)    # добавить upstream-источник
component.add_small_test(name)          # добавить small test (будет запущен вместе с моделью)
component.add_af_callbacks(callbacks)   # добавить callbacks к задачам
component.init_af()                     # создать Airflow-объекты (оператор/TaskGroup)
```

Метод `init_af()` — ключевой: он создаёт реальные Airflow-операторы и TaskGroup, связывает задачи внутри компонента.

---

## 9. Конкретные компоненты DAG

### `DagComponentFactory`
**Файл:** [dmp_af/builder/dmp_af_builder.py](dmp_af/builder/dmp_af_builder.py)

Статическая фабрика — создаёт нужный тип `DagComponent` для `DbtNode`:
```python
DagComponentFactory.create(dbt_node, domain_dag, backfill=False)
```

| Тип узла | `backfill=False` | `backfill=True` |
|---|---|---|
| model | `DagModel` | `BackfillDagModel` |
| snapshot | `DagSnapshot` | `BackfillDagSnapshot` |
| large_test | `LargeTest` | — |
| seed | `DagSeed` | — |

### `DagModel`
Самый частый компонент — запуск одной dbt-модели.

Создаёт в Airflow:
- `TaskGroup` — группа с именем модели
- `DbtRun` (или `DbtKubernetesPodOperator` / `DbtPythonVenvOperator`) — основная задача запуска
- `DbtTest` — запуск small-тестов после модели
- `DbtExternalSensor` или `DbtSourceFreshnessSensor` — ожидание upstream-зависимостей

Выбор оператора определяется типом `Target` из profiles.yml:
- `KubernetesTarget` → `DbtKubernetesPodOperator`
- `VenvTarget` → `DbtPythonVenvOperator`
- иначе → `DbtRun` (BashOperator)

### `DagSnapshot`
Аналогично `DagModel`, но использует `DbtSnapshot` — команда `dbt snapshot`.

### `DagSeed`
Запуск `dbt seed` для CSV-файлов. Использует `DbtSeed`.

### `LargeTest`
Компонент для большого dbt-теста — создаётся отдельный `LargeTestsDomainDag`. Запускается в конце после всех моделей.

### `MediumTests`
Группа тестов, которые запускаются после всех моделей одного домена. Реализованы как `TaskGroup`.

### `BackfillDagModel`
**Файл:** [dmp_af/builder/backfill_dag_components.py](dmp_af/builder/backfill_dag_components.py)

Наследует `DagModel`, но с отличиями:
- `add_external_dependencies = False` — не создаёт сенсоры на upstream-домены.
- `max_active_tis_per_dag = 1` — только один параллельный запуск.
- Имя задачи: `{model_name}__bf`.
- Target берётся из `bf_cluster` или `default_backfill_target`.

### `BackfillDagSnapshot`
Аналог `BackfillDagModel` для снапшотов.

### `MaintenanceDagComponent`
**Файл:** [dmp_af/builder/maintenance_dag_components.py](dmp_af/builder/maintenance_dag_components.py)

Компонент для задач обслуживания таблиц. Создаёт `TaskGroup` и наполняет его задачами через `DbtMaintenanceOperatorFactory`.

Типы обслуживания (`DbtModelMaintenanceType`):
```python
PERSIST_DOCS       # синхронизация описаний колонок
OPTIMIZE_TABLES    # оптимизация таблиц
VACUUM_TABLE       # очистка мёртвых строк
DEDUPLICATE_TABLE  # дедупликация
SET_TTL_ON_TABLE   # удаление устаревших данных (TTL)
```

---

## 10. Операторы Airflow

Все операторы наследуют от `DbtBaseOperator` → `BashOperator` Airflow.

**Файлы:** [dmp_af/operators/](dmp_af/operators/)

### Иерархия операторов:

```
BashOperator (Airflow)
└── DbtBaseOperator          # базовый: формирует bash-команду dbt
    └── DbtBaseActionOperator   # добавляет --select <model>
        └── DbtBaseDatasetOperator  # добавляет поддержку Airflow Dataset
            ├── DbtRun          # dbt run
            ├── DbtSeed         # dbt seed
            ├── DbtSnapshot     # dbt snapshot
            └── DbtTest         # dbt test
```

### `DbtBaseOperator`
Формирует bash-команду:
```bash
cd $PATH_TO_DBT && dbt <command> --profiles-dir $DBT_PROFILES_DIR --project-dir $PATH_TO_DBT --target <target> [--debug]
```

Использует `pool` = `dbt_{target}` для ограничения параллельности (если `use_dbt_target_specific_pools=True`).

### `DbtRun`
`cli_command = 'run'`. Запускает `dbt run --select <model_name>`.

### `DbtSeed`
`cli_command = 'seed'`.

### `DbtSnapshot`
`cli_command = 'snapshot'`.

### `DbtTest`
`cli_command = 'test'`. Запускает small-тесты для модели.

### `DbtKubernetesPodOperator`
**Файл:** [dmp_af/operators/kubernetes_pod.py](dmp_af/operators/kubernetes_pod.py)

Запускает dbt-команду в Kubernetes Pod. Используется когда target в profiles.yml имеет тип `kubernetes`. Настраивает node_pool, image, ресурсы, tolerations.

### `DbtPythonVenvOperator`
**Файл:** [dmp_af/operators/venv.py](dmp_af/operators/venv.py)

Запускает Python-модель dbt в virtualenv. Используется когда target имеет тип `venv`.

### `DbtExternalSensor`
**Файл:** [dmp_af/operators/sensors.py](dmp_af/operators/sensors.py)

Наследует `ExternalTaskSensor` Airflow. Ожидает завершения задачи в другом DAG (в другом домене). Ключевая логика — вычисление правильной `execution_date` upstream-задачи с учётом разных расписаний через `AfExecutionDateFn`.

### `DbtSourceFreshnessSensor`
Проверяет свежесть источника данных через `dbt source freshness`. Срабатывает если данные в источнике устарели.

### `DbtBranchOperator`
**Файл:** [dmp_af/operators/branch.py](dmp_af/operators/branch.py)

`BranchPythonOperator` — пропускает задачу, если она находится за пределами `enable_from_dttm` / `disable_from_dttm`.

---

## 11. Система расписаний (Scheduling)

**Файл:** [dmp_af/common/scheduling.py](dmp_af/common/scheduling.py)

### `BaseScheduleTag` (абстрактный)

Абстрактный класс расписания. Поддерживает `timeshift` — сдвиг запуска относительно стандартного времени.

```python
class BaseScheduleTag(ABC):
    base_name: str           # базовое имя (напр. '@daily')
    name: str                # имя с учётом сдвига
    level: int               # уровень для сравнения
    default_cron_expression  # CronExpression
    af_repr()               # строка для Airflow schedule
```

### `EScheduleTag` (enum-подобная фабрика)

Содержит все стандартные расписания:
```python
EScheduleTag.every15minutes()   # каждые 15 минут
EScheduleTag.hourly()           # каждый час
EScheduleTag.daily()            # каждый день
EScheduleTag.weekly()           # каждую неделю
EScheduleTag.monthly()          # каждый месяц
EScheduleTag.manual()           # без расписания (только ручной запуск)
```

Сдвиг задаётся через параметр `timeshift`:
```python
EScheduleTag.daily(timeshift=timedelta(hours=2))  # в 02:00 вместо 00:00
```

Расписание указывается в YAML-конфиге модели:
```yaml
config:
  dmp_af:
    schedule: "@daily"
```

---

## 12. Зависимости между задачами

### `DagDelayedDependencyRegistry`
**Файл:** [dmp_af/builder/task_dependencies.py](dmp_af/builder/task_dependencies.py)

Откладывает создание зависимостей Airflow (`>>`) до момента, когда все задачи уже созданы. Решает проблему Airflow с race condition при `TaskGroup >> TaskGroup`.

```python
with DagDelayedDependencyRegistry() as delayed_deps:
    delayed_deps(group1) >> delayed_deps(task1)
    delayed_deps(task2) >> delayed_deps(task3)
# при выходе из контекстного менеджера — зависимости применяются в правильном порядке
```

Сначала применяются зависимости `TaskGroup → TaskGroup`, потом `Task → Task`.

### `RegistryDomainDependencies`
**Файл:** [dmp_af/builder/task_dependencies.py](dmp_af/builder/task_dependencies.py)

Реестр сенсоров для ожидания задач из другого домена. При политике `per_domain` — все сенсоры для одного upstream-домена группируются в один `TaskGroup`.

### `AfExecutionDateFn`
**Файл:** [dmp_af/operators/sensors.py](dmp_af/operators/sensors.py)

Вычисляет правильную `execution_date` для `ExternalTaskSensor` с учётом разных расписаний upstream и downstream. Например, если upstream запускается раз в час, а downstream — раз в день, сенсор должен ждать последнего часового запуска.

---

## 13. Функции-сборщики DAG

**Файл:** [dmp_af/dags.py](dmp_af/dags.py)

### `_compile_dbt_dags(manifest_content, profiles, project_profile_name, config, etl_service_name)`

Внутренняя функция. Создаёт `DmpAfGraph`, затем:
1. Вызывает `dbt_main_dags(graph)` — основные плановые DAG.
2. Если `config.include_single_model_manual_dag=True` — вызывает `dbt_run_model_dag(config)`.

### `dbt_main_dags(graph: DmpAfGraph) → dict[str, DAG]`

Создаёт Airflow `DAG` для каждого `DomainDag` в графе:

1. **Для каждого `DomainDag`** из `graph.nodes`:
   - Создаёт `DAG` с именем, расписанием, тегами, callbacks.
   - Для `BackfillDomainDag` — вызывает `wrap_dag_with_endpoints()`.

2. **Для каждого узла** (`DagComponent`):
   - Добавляет task-callbacks.
   - Вызывает `node.init_af()` — создаёт Airflow-задачи.

3. **Для Backfill-узлов**:
   - Соединяет `start_endpoint >> первые задачи без upstream`.

### `dbt_run_model_dag(config: Config) → dict[str, DAG]`

Создаёт специальный DAG для ручного запуска произвольной модели:
- Имя: `{project}_dbt_run_model`
- Расписание: `None` (только ручной запуск)
- Параметры (Form UI в Airflow):
  - `dbt_select_model` — selector для выбора моделей (поддерживает `+model+`, `tag:...` и т. д.)
  - `start_dttm`, `end_dttm` — временной интервал
  - `target` — опциональный override target
  - `full-refresh` — флаг полного пересчёта
  - `other_dbt_cli_options` — произвольные аргументы CLI

---

## 14. Интеграции и коллбэки

**Файл:** [dmp_af/common/af_callbacks.py](dmp_af/common/af_callbacks.py)

`collect_af_custom_callbacks(config)` собирает callbacks из всех активных интеграций:

### Встроенные интеграции:

#### Monte Carlo Data (MCD)
**Папка:** [dmp_af/integrations/mcd/](dmp_af/integrations/mcd/)

При `config.mcd.callbacks_enabled=True` — добавляет callbacks для отправки метрик в MCD.
При `config.mcd.artifacts_export_enabled=True` — экспортирует артефакты dbt в MCD после прогона.

#### Tableau
**Папка:** [dmp_af/integrations/tableau/](dmp_af/integrations/tableau/)

Обновляет Tableau-экстракты (workbook или datasource) после выполнения модели.
Конфигурируется через `config.tableau` и `node.config.tableau_refresh`.

#### Custom Airflow Callbacks
**Папка:** [dmp_af/integrations/af_callbacks/](dmp_af/integrations/af_callbacks/)

Применяет пользовательские callbacks из `config.custom_af_callbacks`.

---

## 15. Полная последовательность вызовов

```
compile_dmp_af_dags(manifest_path, config, etl_service_name)
│
├─ open(manifest.json) → manifest: dict
├─ open(profiles.yml) → profiles: dict
├─ open(dbt_project.yml) → project_profile_name: str
│
└─ _compile_dbt_dags(manifest, profiles, project_profile_name, config, etl_service_name)
   │
   ├─ DmpAfGraph.from_manifest(manifest, profiles, project_profile_name, config, etl_service_name)
   │  │
   │  ├─ Profiles(**profiles)[project_profile_name] → Profile
   │  ├─ [DbtNode(**node_info) for each node in manifest['nodes']]
   │  │    └─ node.set_target_details(profile, config.dbt_default_targets)
   │  ├─ [DbtSource(**source_info) for each source in manifest['sources']]
   │  │
   │  └─ graph._build_dags()
   │     │
   │     ├─ _build_dag_components(nodes)
   │     │  ├─ _collect_all_models(nodes)
   │     │  │  └─ DagComponentFactory.create(node, domain_dag)
   │     │  │     ├─ DomainDagsRegistry.get(node) → DomainDag
   │     │  │     └─ DagModel | DagSnapshot | LargeTest | DagSeed
   │     │  ├─ _collect_maintenance_components()
   │     │  │  └─ MaintenanceDagComponent(domain_dag, maintenance_type)
   │     │  ├─ _resolve_dependencies(nodes)
   │     │  │  ├─ model.add_dependency(upstream_model)
   │     │  │  └─ model.add_source_dependency(source)
   │     │  └─ _bind_medium_tests()
   │     │
   │     └─ _build_backfill_dag_components(nodes)
   │        ├─ _collect_all_models(nodes, backfill=True)
   │        │  └─ BackfillDagModel | BackfillDagSnapshot
   │        └─ _resolve_dependencies(nodes, backfill=True)
   │
   ├─ dbt_main_dags(graph)
   │  │
   │  ├─ collect_af_custom_callbacks(config) → dag_callbacks, task_callbacks
   │  │
   │  ├─ for domain_dag in domains:
   │  │     DAG(domain_dag.dag_name, schedule, tags, ...) → af_dag
   │  │     domain_dag.af_dag = af_dag
   │  │     if BackfillDomainDag: domain_dag.wrap_dag_with_endpoints()
   │  │
   │  ├─ for node in graph.nodes:
   │  │     node.add_af_callbacks(task_callbacks)
   │  │     node.init_af()   ← создаёт реальные Airflow-задачи
   │  │
   │  └─ for backfill node: start_endpoint >> node.af_component
   │
   └─ dbt_run_model_dag(config)    ← если include_single_model_manual_dag=True
      └─ DAG(schedule=None) + DbtRun(model_name=None)
```

---

## Карта зависимостей между модулями

```
dags.py
  ├── conf/config.py          (Config и все вложенные конфиги)
  ├── builder/
  │     ├── dmp_af_builder.py (DmpAfGraph, DomainDagsRegistry, DagComponentFactory)
  │     │     ├── domain_dag.py        (DomainDag и подклассы, DomainDagFactory)
  │     │     ├── dag_components.py    (DagComponent, DagModel, DagSnapshot, ...)
  │     │     ├── backfill_dag_components.py (BackfillDagModel, BackfillDagSnapshot)
  │     │     ├── maintenance_dag_components.py (MaintenanceDagComponent)
  │     │     └── task_dependencies.py (DagDelayedDependencyRegistry, RegistryDomainDependencies)
  │     └── dbt_model_path_graph_builder.py
  ├── parser/
  │     ├── dbt_node_model.py   (DbtNode, DbtNodeConfig, WaitPolicy, ...)
  │     ├── dbt_profiles.py     (Profiles, Profile, KubernetesTarget, VenvTarget)
  │     └── dbt_source_model.py (DbtSource, DbtSourceFreshness)
  ├── operators/
  │     ├── base.py             (DbtBaseOperator)
  │     ├── run.py              (DbtRun, DbtSeed, DbtSnapshot, DbtTest)
  │     ├── kubernetes_pod.py   (DbtKubernetesPodOperator)
  │     ├── venv.py             (DbtPythonVenvOperator)
  │     ├── sensors.py          (DbtExternalSensor, DbtSourceFreshnessSensor, AfExecutionDateFn)
  │     ├── branch.py           (DbtBranchOperator)
  │     ├── macros.py           (DbtMaintenanceOperatorFactory)
  │     └── supplemental.py     (TableauExtractsRefreshOperator)
  ├── common/
  │     ├── scheduling.py       (BaseScheduleTag, EScheduleTag)
  │     ├── af_callbacks.py     (collect_af_custom_callbacks)
  │     ├── af_scheduling_utils.py
  │     ├── constants.py
  │     ├── cron.py
  │     └── utils.py
  └── integrations/
        ├── mcd/                (Monte Carlo Data)
        ├── tableau/            (Tableau)
        └── af_callbacks/       (Custom Airflow callbacks)
```
