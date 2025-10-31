# Architecture Overview

## System Design

TG Gifts Notifier follows a modular, production-ready architecture with clear separation of concerns.

---

## Component Architecture

### Core Layer

#### `detector.py`
**Role**: Main orchestration and monitoring engine

**Responsibilities**:
- Coordinates all monitoring activities
- Manages asyncio tasks
- Handles data persistence
- Applies filters and priorities
- Orchestrates notifications

**Key Functions**:
- `detector()` - Main monitoring loop
- `process_new_gift()` - Handle new gift notifications
- `process_update_gifts()` - Handle gift updates
- `star_gifts_upgrades_checker()` - Check for upgradable gifts

#### `star_gifts_data.py`
**Role**: Data models and persistence

**Responsibilities**:
- Define data structures
- Handle JSON serialization
- Manage file I/O
- Store historical data

**Models**:
- `StarGiftHistoryEntry` - History snapshot
- `StarGiftData` - Gift data with history
- `StarGiftsData` - Collection wrapper

#### `parse_data.py`
**Role**: Telegram API integration

**Responsibilities**:
- Interface with MTProto API
- Handle API responses
- Transform raw data to models
- Optimize with hash-based caching

**Key Functions**:
- `get_all_star_gifts()` - Fetch gifts with caching
- `check_is_star_gift_upgradable()` - Check upgrade status

---

### Feature Layer

#### `history_manager.py`
**Role**: Historical analytics engine

**Responsibilities**:
- Track all gift changes
- Calculate sale rates
- Predict sold-out times
- Generate statistics
- Detect critical patterns

**Key Functions**:
- `add_history_entry()` - Record changes
- `get_sale_rate()` - Calculate velocity
- `get_estimated_sold_out_time()` - Predict timeline
- `is_critical_sale_speed()` - Flag fast sales
- `calculate_statistics()` - Generate metrics

**Data Models**:
- `GiftStatistics` - Per-gift metrics
- `GlobalStatistics` - System-wide stats

#### `filters_and_priorities.py`
**Role**: Intelligent notification system

**Responsibilities**:
- Apply user-defined filters
- Calculate notification priorities
- Route notifications by priority
- Support blacklists and whitelists

**Components**:
- `NotificationPriority` - Priority enum
- `FilterConfig` - Filter configuration
- `check_filter()` - Filter evaluation
- `calculate_priority()` - Priority calculation
- `should_send_notification()` - Decision logic

**Priority Levels**:
1. **CRITICAL** (>50% sold/hour)
2. **HIGH** (rare or >25% sold/hour)
3. **NORMAL** (standard gifts)
4. **LOW** (non-limited, non-upgradable)

#### `backup_manager.py`
**Role**: Data protection layer

**Responsibilities**:
- Create point-in-time backups
- Manage backup retention
- Restore from backups
- Automatic scheduling

**Key Functions**:
- `create_backup()` - Generate backup
- `restore_from_backup()` - Recover data
- `list_backups()` - Enumerate backups
- `cleanup_old_backups()` - Retention policy
- `auto_backup_task()` - Scheduled backups

#### `api_server.py`
**Role**: REST API interface

**Responsibilities**:
- Provide HTTP API
- Serve real-time data
- Generate analytics on-demand
- Enable integrations

**Endpoints**:
- Resource endpoints (`/gifts`, `/gifts/{id}`)
- History endpoints (`/gifts/{id}/history`)
- Statistics endpoints (`/statistics`, `/gifts/{id}/statistics`)
- Health endpoint (`/health`)

**Technology**:
- FastAPI framework
- Automatic OpenAPI docs
- Async request handling

---

### Utility Layer

#### `utils.py`
**Role**: Common utilities

**Responsibilities**:
- Logging configuration
- Time formatting
- Number formatting
- Date/time utilities

**Key Functions**:
- `get_logger()` - Logger factory
- `get_current_datetime()` - Time formatting
- `get_current_timestamp()` - Unix timestamp
- `pretty_int()` / `pretty_float()` - Number formatting
- `format_seconds_to_human_readable()` - Duration formatting

#### `cli.py`
**Role**: Command-line interface

**Responsibilities**:
- Provide user-friendly CLI
- Enable data export
- Support batch operations
- Facilitate debugging

**Commands**:
- Search and list operations
- History inspection
- CSV export
- Statistics display
- Backup management

#### `config_validator.py`
**Role**: Configuration validation

**Responsibilities**:
- Validate settings at startup
- Prevent runtime errors
- Provide clear error messages
- Ensure data integrity

**Validation**:
- API credentials
- Bot token format
- Interval values
- File paths
- Timezone validity

#### `userbot_helpers.py`
**Role**: Media handling

**Responsibilities**:
- Download sticker files
- Handle multiple DCs
- Support CDN downloads
- Optimize batch operations

---

## Data Flow

### Monitoring Flow

```
1. Detector Loop
   ↓
2. Telegram API Request (parse_data.py)
   ↓
3. Data Parsing & Caching
   ↓
4. Compare with Stored Data
   ↓
5. Filter Application (filters_and_priorities.py)
   ↓
6. Priority Calculation
   ↓
7. Notification Decision
   ↓
8. Send Notification (if needed)
   ↓
9. Add History Entry (history_manager.py)
   ↓
10. Persist Data (star_gifts_data.py)
```

### Update Flow

```
1. Detector detects change
   ↓
2. Queue update request
   ↓
3. Edit existing message
   ↓
4. Add history entry
   ↓
5. Persist change
```

### Upgrade Check Flow

```
1. Upgrade Checker Loop
   ↓
2. Check gift upgradability
   ↓
3. If upgradable:
   ↓
   a. Download sticker
   b. Send notification
   c. Update status
   d. Add history entry
```

---

## Design Patterns

### 1. **Observer Pattern**
- Detector observes Telegram API
- Filters observe detected gifts
- History manager observes changes

### 2. **Factory Pattern**
- Logger factory in `utils.py`
- Data model factories in `star_gifts_data.py`

### 3. **Strategy Pattern**
- Filter strategies in `filters_and_priorities.py`
- Priority strategies

### 4. **Repository Pattern**
- `star_gifts_data.py` acts as data repository
- Abstracted persistence layer

### 5. **Builder Pattern**
- Configuration builders in `config_validator.py`
- Filter configuration builder

---

## Security Considerations

### Data Protection
- ✅ No hardcoded credentials
- ✅ Configuration file excluded from git
- ✅ Session files in .gitignore
- ✅ Secure API key handling

### Error Handling
- ✅ Graceful degradation
- ✅ Detailed error logging
- ✅ Retry mechanisms
- ✅ Connection recovery

### Rate Limiting
- ✅ Configurable intervals
- ✅ Bot token rotation
- ✅ Request queuing

---

## Scalability

### Performance Optimizations
- Hash-based API caching
- Batch sticker downloads
- Async operations throughout
- Efficient data structures
- History size limiting (1000 entries)

### Resource Management
- Rotating file logs (10MB × 1000 files)
- Automatic backup cleanup
- Memory-efficient models
- Lazy loading where possible

### Horizontal Scaling
- Stateless API server
- JSON-based storage (easily distributable)
- No shared state
- Can run multiple instances

---

## Testing Strategy

### Unit Tests
- Utility functions (`test_utils.py`)
- Validators (`test_config_validator.py`)
- Individual components

### Integration Tests
- API endpoints
- Data persistence
- Filter application

### End-to-End Tests
- Full monitoring cycle
- Notification delivery
- Backup/restore

---

## Deployment

### Production Considerations

**System Requirements**:
- Python 3.10+
- Persistent storage for JSON files
- Network access to Telegram API
- Optional: Systemd service

**Monitoring**:
- Use `/health` endpoint
- Monitor log files
- Track backup success
- Alert on errors

**Backup Strategy**:
- Hourly automated backups
- 24+ hour retention
- Test restore procedures
- Off-site backups recommended

---

## Future Enhancements

### Planned Features
- Database backend (PostgreSQL/MongoDB)
- Web dashboard
- Webhook notifications
- Multi-user support
- Advanced ML predictions
- Rate limiting improvements
- Authentication for API

### Performance Improvements
- Redis caching
- Message queue (RabbitMQ)
- Connection pooling
- Query optimization

---

## Technology Stack

### Core
- **Python**: 3.10+
- **Pyrogram**: MTProto client
- **Pydantic**: Data validation
- **AsyncIO**: Concurrency

### API Layer
- **FastAPI**: REST framework
- **Uvicorn**: ASGI server
- **Pydantic**: Request/response models

### Utilities
- **Pytz**: Timezone handling
- **NumPy**: Numeric operations
- **SimpleJSON**: Fast JSON
- **HTTPX**: HTTP client

### Development
- **pytest**: Testing
- **pytest-asyncio**: Async tests

---

## Module Dependencies

```
detector.py
  ├── parse_data.py
  ├── star_gifts_data.py
  ├── utils.py
  ├── userbot_helpers.py
  ├── config_validator.py
  ├── history_manager.py
  ├── filters_and_priorities.py
  └── config.py

api_server.py
  ├── star_gifts_data.py
  ├── history_manager.py
  ├── utils.py
  ├── constants.py
  └── config.py

cli.py
  ├── star_gifts_data.py
  ├── history_manager.py
  ├── backup_manager.py
  ├── utils.py
  ├── constants.py
  └── config.py

backup_manager.py
  ├── star_gifts_data.py
  ├── constants.py
  └── config.py
```

---

## Best Practices

### Code Quality
- ✅ Type hints throughout
- ✅ Comprehensive docstrings
- ✅ Modular design
- ✅ Error handling
- ✅ Logging at all levels

### Configuration
- ✅ Validation at startup
- ✅ Sensible defaults
- ✅ Clear documentation
- ✅ Example config file

### Documentation
- ✅ README for users
- ✅ IMPROVEMENTS for features
- ✅ Architecture docs
- ✅ API docs (auto-generated)

---

This architecture supports production deployment with enterprise-grade features while maintaining code clarity and extensibility.

