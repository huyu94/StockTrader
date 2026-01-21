---

description: "Task list template for feature implementation"
---

# Tasks: Web Dashboard

**Input**: Design documents from `/specs/001-web-dashboard/`  
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: 本需求未要求测试任务，本清单不包含测试项。  

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Web app**: `backend/src/`, `frontend/src/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create `backend/src/`, `backend/tests/`, `frontend/src/`, `frontend/tests/` directories
- [x] T002 Initialize backend app entry in `backend/src/main.py`
- [x] T003 Initialize frontend app entry in `frontend/src/main.tsx` and `frontend/src/App.tsx`
- [x] T004 [P] Add frontend build configs in `frontend/package.json`, `frontend/tsconfig.json`, `frontend/vite.config.ts`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Configure backend settings loader in `backend/src/services/settings.py`
- [x] T006 Configure database session in `backend/src/services/database.py`
- [x] T007 Add common response schemas in `backend/src/models/common.py`
- [x] T008 Setup API router and health endpoint in `backend/src/api/router.py` and `backend/src/api/health.py`
- [x] T009 Configure loguru logging in `backend/src/services/logging.py`
- [x] T010 Create frontend HTTP client in `frontend/src/services/httpClient.ts`
- [x] T011 Build app layout shell in `frontend/src/components/AppLayout.tsx`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - 日线行情可视化 (Priority: P1) 🎯 MVP

**Goal**: 提供日线图展示与空状态处理

**Independent Test**: 选择一个有日线数据的股票并查看图表；无数据时提示空状态

### Implementation for User Story 1

- [x] T012 [P] [US1] Create daily kline schema in `backend/src/models/daily_kline.py`
- [x] T013 [US1] Implement daily kline service in `backend/src/services/daily_kline_service.py`
- [x] T014 [US1] Implement daily kline endpoint in `backend/src/api/daily_kline.py`
- [x] T015 [P] [US1] Create daily kline API client in `frontend/src/services/dailyKlineApi.ts`
- [x] T016 [US1] Build chart component in `frontend/src/components/DailyKlineChart.tsx`
- [x] T017 [US1] Build daily kline page in `frontend/src/pages/DailyKlinePage.tsx`

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - 股票搜索与列表 (Priority: P2)

**Goal**: 支持股票搜索与列表浏览并跳转日线页

**Independent Test**: 输入股票代码或名称可返回结果并进入日线页面

### Implementation for User Story 2

- [x] T018 [P] [US2] Create stock schema in `backend/src/models/stock.py`
- [x] T019 [US2] Implement stock search service in `backend/src/services/stock_service.py`
- [x] T020 [US2] Implement stock search endpoint in `backend/src/api/stocks.py`
- [x] T021 [P] [US2] Create stock API client in `frontend/src/services/stockApi.ts`
- [x] T022 [US2] Build stock search components in `frontend/src/components/StockSearchBar.tsx` and `frontend/src/components/StockList.tsx`
- [x] T023 [US2] Build dashboard page in `frontend/src/pages/DashboardPage.tsx`

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - 预留数据采集按钮区 (Priority: P3)

**Goal**: 展示固定位置的预留采集按钮区

**Independent Test**: 页面加载后按钮区可见，点击显示未启用提示

### Implementation for User Story 3

- [x] T024 [P] [US3] Create action slot schema in `backend/src/models/ui_action_slot.py`
- [x] T025 [US3] Implement action slot service in `backend/src/services/ui_action_slot_service.py`
- [x] T026 [US3] Implement action slot endpoint in `backend/src/api/ui_action_slots.py`
- [x] T027 [P] [US3] Create action slot API client in `frontend/src/services/actionSlotApi.ts`
- [x] T028 [US3] Build action slot bar in `frontend/src/components/ActionSlotBar.tsx`

**Checkpoint**: User Story 3 independently functional

---

## Phase 6: User Story 4 - 数据更新状态展示 (Priority: P4)

**Goal**: 展示数据更新时间与覆盖范围

**Independent Test**: 打开页面即可看到更新状态信息

### Implementation for User Story 4

- [x] T029 [P] [US4] Create data update status schema in `backend/src/models/data_update_status.py`
- [x] T030 [US4] Implement data update status service in `backend/src/services/data_update_status_service.py`
- [x] T031 [US4] Implement data update status endpoint in `backend/src/api/data_update_status.py`
- [x] T032 [P] [US4] Create data status API client in `frontend/src/services/dataStatusApi.ts`
- [x] T033 [US4] Build data status panel in `frontend/src/components/DataUpdateStatusPanel.tsx`

**Checkpoint**: All user stories should now be independently functional

---

## Phase N: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T034 [P] Align API docs with implementation in `specs/001-web-dashboard/contracts/api.yaml`
- [ ] T035 [P] Update quickstart commands in `specs/001-web-dashboard/quickstart.md`
- [ ] T036 Code cleanup and refactoring in `backend/src/` and `frontend/src/`
- [ ] T037 Performance tuning for daily kline queries in `backend/src/services/daily_kline_service.py`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3+)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3 → P4)
- **Polish (Final Phase)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with US1 but should be independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - Independent of US1/US2
- **User Story 4 (P4)**: Can start after Foundational (Phase 2) - Independent of US1/US2

### Within Each User Story

- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- Setup tasks marked [P] can run in parallel
- Foundational tasks marked [P] can run in parallel (within Phase 2)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- Backend tasks can run in parallel with frontend tasks for the same story

---

## Parallel Example: User Story 1

```bash
Task: "Create daily kline schema in backend/src/models/daily_kline.py"
Task: "Create daily kline API client in frontend/src/services/dailyKlineApi.ts"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Add User Story 4 → Test independently → Deploy/Demo
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
   - Developer D: User Story 4
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence
