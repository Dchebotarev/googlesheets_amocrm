<?php
// die;
header('Content-Type: text/html; charset=utf-8');
ini_set('error_log', dirname(__DIR__) . '/' . 'transferlogs/' . date('Y-m-d') . 'crontransfer1.log');

require dirname(dirname(dirname(dirname(__DIR__)))) . '/vendor/autoload.php';
require_once dirname(__DIR__) . '/include/DatabaseClass.php';
require_once dirname(__DIR__) . '/config.php';

use League\OAuth2\Client\Token\AccessToken;
use AmoCRM\Client\AmoCRMApiClient;
use AmoCRM\Filters\TasksFilter;
use AmoCRM\Models\ContactModel;
use AmoCRM\Exceptions\AmoCRMApiException;
use AmoCRM\Filters\ContactsFilter;
use AmoCRM\Filters\NotesFilter;
use AmoCRM\Filters\EventsFilter;
use AmoCRM\Models\Factories\NoteFactory;
use AmoCRM\Helpers\EntityTypesInterface;
use AmoCRM\Filters\BaseRangeFilter;
use AmoCRM\Collections\CustomFields\CustomFieldsCollection;
use AmoCRM\Models\CustomFields\TextCustomFieldModel;


function putContent($var, $line)
{
    $file = dirname(__DIR__) . '/' . 'logs/' . date('Y-m-d') . 'cron-1.log';
    file_put_contents(
        $file,
        $line . date(' d H:i:s ') . $var . "\n",
        FILE_APPEND
    );
}

function findIndexInArray($arr, $name)
{
    if ($arr[0]) {
        foreach ($arr as $index => $value) {
            if (is_array($value)) {
                if ($value['name'] === $name) {
                    return $name;
                }
            }
        }
    }

    return null;
}

function printError(AmoCRMApiException $e, $line): void
{
    $errorTitle = $e->getTitle();
    $code = $e->getCode();
    // $debugInfo = var_export($e->getLastRequestInfo(), true);

    $error = <<<EOF
        Error: $errorTitle
        Code: $code
    EOF;

    putContent($error, $line);
}

// Обратить внимание, сущности обновляются по последней дате обновления
// В конце данного скрипта в файл contacttime.txt пишется переменная $nowtime
// Крон настроен каждые 10 минут
date_default_timezone_set('Europe/Moscow');
$start = strtotime(date("Y-m-d") . " 00:00:00");
$end = strtotime(date("Y-m-d") . " 23:59:59");

$updatetime = file_get_contents(__DIR__ . '/contacttime.txt', true);
$updatetime = (int)$updatetime - 60;
$nowtime = time();

putContent('Работает начало ' . date("H:i:s", $updatetime) . ' ', __LINE__);

/** База данных */
$database = new Database(DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_CHARSET, DB_TABLE_PREFIX);

$subdomain = 'oppopart';
$creds = $database->select(
    'amo_users',
    '*',
    'subdomain LIKE "%' . $subdomain . '%"'
);

$apiClient = new AmoCRMApiClient(
    $creds[0]['client_id'],
    $creds[0]['client_secret'],
    $creds[0]['redirect_uri']
);

$apiClient->setAccountBaseDomain($subdomain . '.amocrm.ru');
$apiClient->setAccessToken(new AccessToken([
    'access_token' => $creds[0]['access_token'],
    'refresh_token' => $creds[0]['refresh_token'],
    'token_type' => "Bearer",
    'expires' => strtotime('+4 hours')
]));

// Подключаем Google_Sheets
putenv('GOOGLE_APPLICATION_CREDENTIALS=' . dirname(__DIR__) . '/*.json');

// Документация https://developers.google.com/sheets/api/
// Список всех возможных ошибок и исключений https://developers.google.com/drive/v3/web/handle-errors
$client = new Google_Client();
$client->useApplicationDefaultCredentials();
$client->addScope('https://www.googleapis.com/auth/spreadsheets');
$service = new Google_Service_Sheets($client);

// ID таблицы, получаем информацию о листе
$spreadsheetId = '1vOt7d8NfCdeerI4AYEap7KUisaf_7HnE2e2g3Xe-_4E';
$response = $service->spreadsheets->get($spreadsheetId);
foreach ($response->getSheets() as $sheet) {
    $sheetProperties = $sheet->getProperties();
    if ($sheetProperties->index === 0) {
        $sheetPropertiesTitle = $sheetProperties->title; // Название листа
        $sheetPropertiesId = $sheetProperties->sheetId; // id листа
    }
}

// Наименования столбцов в google таблице
$range = $sheetPropertiesTitle . '!A1:AZ1';
$responseSpreadsheetsValues = $service->spreadsheets_values->get($spreadsheetId, $range);
$googleFieldsLineNames = $responseSpreadsheetsValues->values[0];

$page = 1;
$pass = false;
$newEventsArray = [];
while ($pass === false) {
    // Получим удаленные контакты по событию, чтобы удалить их из гугл таблицы
    $filterEvents = new EventsFilter();
    $filterEvents->setCreatedAt([$start, $end]);
    $filterEvents->setTypes(['contact_deleted']);
    $filterEvents->setEntity(['contact']);
    $filterEvents->setLimit(50);
    $filterEvents->setPage($page);

    unset($events);
    try {
        $events = $apiClient->events()->get($filterEvents);
        if (!empty($events)) {
            foreach ($events as $event) {
                $newEventsArray[] = $event->entityId;
            }
        }
        $page++;
    } catch (AmoCRMApiException $e) {
        // printError($e, __LINE__);
        if ($e->getCode() === 204) {
            $pass = true;
        }
    }
}

putContent('Количество под удаление ' . sizeof($newEventsArray) . ' - ' . json_encode($newEventsArray), __LINE__);
// Получим строки сущностей в гугл таблице и удалим их пакетом
if (!empty($newEventsArray)) {
    $idColumn = $service->spreadsheets_values->get($spreadsheetId, $sheetPropertiesTitle . '!A2:A', ['valueRenderOption' => 'UNFORMATTED_VALUE']);
    $requests = [];
    $index = 0;
    foreach ($newEventsArray as $k => $newEvent) {
        if ($idColumn['values'][0]) {
            $row = array_keys($idColumn['values'], [$newEvent]);
            // putContent($row, __LINE__);
            if (!empty($row)) {
                $rowId = $row[0] - $index;
                if ($rowId >= 0) {
                    $requests[] = new Google_Service_Sheets_Request([
                        'deleteDimension' => [
                            'range'          => [
                                'sheetId' => (int)$sheetPropertiesId,
                                'dimension' => "ROWS",
                                "startIndex" => $rowId + 1,
                                "endIndex" => $rowId + 2
                            ],
                        ]
                    ]);
                    $index++;
                }
            }
        }
    }
    putContent('Массив удаление, поиск строк ' . json_encode($requests), __LINE__);

    if (!empty($requests)) {

        $batchUpdateRequest = new Google_Service_Sheets_BatchUpdateSpreadsheetRequest([
            'requests' => $requests
        ]);

        try {
            $response = $service->spreadsheets->batchUpdate($spreadsheetId, $batchUpdateRequest);
        } catch (Google_Service_Exception $exception) {
            $reason = $exception->getErrors()[0]['reason'];
            // putContent(json_encode($exception->getErrors(), JSON_UNESCAPED_UNICODE), __LINE__);
        }
    }
}

// Получим контакты по дате обновления
$range = new BaseRangeFilter();
$range->setFrom($updatetime);
$range->setTo($nowtime);

$filter = new ContactsFilter();
$filter->setUpdatedAt($range);
$filter->setLimit(250);
$filter->setOrder('updated_at','desc');

try {
    $contacts = $apiClient->contacts()->get($filter, [ContactModel::LEADS]);
    putContent('10 минут контакты - ' . sizeof($contacts->toArray()), __LINE__);
} catch (AmoCRMApiException $e) {
    putContent('Нет измененных от ' . date("H:i:s", $updatetime) . ' ', __LINE__);
}

$googleSheetsValues = [];
if ($contacts[0]) {

    $valuesKeys = [
        "ID",
        "ФИО",
        "Название компании",
        "Кто изменил",
        "Время изменения",
        "Кто создал",
        "Время создания",
        "Теги",
        "Ответственный",
        "Связанные сделки",
        "Ближайшая задача",
        "Примечание 1"
    ];

    // Получим кастомные поля всех контактов
    try {
        $customFieldsService = $apiClient->customFields(EntityTypesInterface::CONTACTS);
        $results = $customFieldsService->get()->toArray();
    } catch (AmoCRMApiException $e) {
        // printError($e, __LINE__);
    }

    // Получим id сущностей в гугл таблице
    $idColumn = $service->spreadsheets_values->get($spreadsheetId, $sheetPropertiesTitle . '!A2:A', ['valueRenderOption' => 'UNFORMATTED_VALUE']);

    $users = $apiClient->users()->get();

    foreach ($contacts as $k => $contact) {
        if ($contact->id === null) continue;

        if ($contact->company !== null) {
            $company = $apiClient->companies()->getOne($contact->company->id);
            $contactsCompanyName = $company->name;
        } else {
            $contactsCompanyName = '';
        }

        $updated_by = null;
        foreach ($users as $user) {
            if ($user->id === $contact->updated_by && $contact->updated_by !== 0) {
                $updated_by = $user->name;
                break;
            }
        }

        $updated_at = date("Y-m-d H:i:s", $contact->updated_at);

        $created_by = null;
        foreach ($users as $user) {
            if ($user->id === $contact->created_by && $contact->created_by !== 0) {
                $created_by = $user->name;
                break;
            }
        }

        $created_at = date("Y-m-d H:i:s", $contact->created_at);

        $tags = null;
        try {
            $tags = $contact->getTags();
        } catch (AmoCRMApiException $e) {
            // printError($e, __LINE__);
        }
        $tagsValues = '';
        if ($tags[0]) {
            foreach ($tags as $tag) {
                $tagsValues .= $tag->name . "; ";
            }
        }

        $responsible = null;
        foreach ($users as $user) {
            if ($user->id === $contact->responsible_user_id && $contact->responsible_user_id !== 0) {
                $responsible = $user->name;
                break;
            }
        }

        $leadName = '';
        try {
            $leadsCollections = $contact->getLeads();
            if ($leadsCollections[0]) {
                foreach ($leadsCollections as $lead) {
                    try {
                        $leadInfo = $apiClient->leads()->getOne($lead->id);
                        if ($leadInfo->name) {
                            $leadName .= $leadInfo->name . "; ";
                        }
                    } catch (AmoCRMApiException $e) {
                        // printError($e, __LINE__);
                    }
                }
            }
        } catch (AmoCRMApiException $e) {
            // printError($e, __LINE__);
        }

        $taskFilter = new TasksFilter;
        $taskFilter->setEntityType('contacts')->setEntityIds($contact->id)->setIsCompleted(false);

        $taskTimes = '';
        try {
            $tasks = $apiClient->tasks()->get($taskFilter);
            if ($tasks[0]) {
                foreach ($tasks as $task) {
                    if ($task !== null) {
                        $taskTimes .= date("Y-m-d H:i:s", $task->complete_till) . "; ";
                    }
                }
            }
        } catch (AmoCRMApiException $e) {
            // printError($e, __LINE__);
        }

        // Собираем значения в массив
        $googleSheetsValues[$k][] = $contact->id;
        $googleSheetsValues[$k][] = $contact->name ?: '';
        $googleSheetsValues[$k][] = $contactsCompanyName;
        $googleSheetsValues[$k][] = $contact->updated_by === 0 ? 'Робот' : ($updated_by === null ? 'Пользователь удален' : $updated_by);
        $googleSheetsValues[$k][] = $updated_at ?: '';
        $googleSheetsValues[$k][] = $contact->created_by === 0 ? 'Робот' : ($created_by === null ? 'Пользователь удален' : $created_by);
        $googleSheetsValues[$k][] = $created_at ?: '';
        $googleSheetsValues[$k][] = $tagsValues;
        $googleSheetsValues[$k][] = $contact->responsible_user_id === 0 ? 'Робот' : ($responsible === null ? 'Пользователь удален' : $responsible);
        $googleSheetsValues[$k][] = $leadName;
        $googleSheetsValues[$k][] = $taskTimes;

        $customFields = null;
        // Кастомные поля контакта
        try {
            $customFields = $contact->getCustomFieldsValues();
        } catch (AmoCRMApiException $e) {
            // printError($e, __LINE__);
        }

        // Прогоняем наименования столбцов таблицы
        foreach ($googleFieldsLineNames as $key => $googleFieldsLineName) {
            if (!in_array($googleFieldsLineName, $valuesKeys)) {
                // Гугл таблицы так работают, каждый элемент массива это одна ячейка
                $googleSheetsValues[$k][$key] = '';

                // Ищем дополнительное поле в amoCRM и если такого поля нет, то создадим его в amoCRM
                if (findIndexInArray($results, $googleFieldsLineName) === null) {
                    // $customFieldsCollection = new CustomFieldsCollection();
                    // $cf = new TextCustomFieldModel();
                    // $cf->setName($googleFieldsLineName);

                    // $customFieldsCollection->add($cf);

                    // try {
                    //     $customFieldsCollection = $customFieldsService->add($customFieldsCollection);
                    // } catch (AmoCRMApiException $e) {
                    //     printError($e, __LINE__);
                    // }
                } else {
                    $fieldName = null;
                    if ($customFields !== null) {
                        try {
                            $fieldName = $customFields->getBy('fieldName', $googleFieldsLineName);
                        } catch (AmoCRMApiException $e) {
                            // printError($e, __LINE__);
                        }
                    }
                    if ($fieldName !== null) {
                        $fieldValue = $fieldName->getValues()->first()->value;
                        if (is_bool($fieldValue)) {
                            if ($fieldValue === true) {
                                $googleSheetsValues[$k][$key] = 'Да';
                            } else {
                                $googleSheetsValues[$k][$key] = 'Нет';
                            }
                        } else {
                            $googleSheetsValues[$k][$key] = $fieldValue;
                        }
                    }
                }
            } else {
                // Гугл таблицы так работают, каждый элемент массива это одна ячейка
                if ($key > 10) {
                    $googleSheetsValues[$k][$key] = '';
                }
            }
        }

        $valuesCount = count($googleSheetsValues[$k]);
        $notesValues = '';
        try {
            $contactsNotesService = $apiClient->notes(EntityTypesInterface::CONTACTS);
            $notesCollection = $contactsNotesService->getByParentId($contact->id, (new NotesFilter())->setNoteTypes([NoteFactory::NOTE_TYPE_CODE_COMMON]));
            $notesCollections = $notesCollection->toArray();
            if ($notesCollections[0]) {
                foreach ($notesCollections as $note) {
                    if ($note['params']['text']) {
                        $notesValues .= $note['params']['text'] . "; ";
                    }
                }
            }
        } catch (AmoCRMApiException $e) {
            // printError($e, __LINE__);
        }

        $googleSheetsValues[$k][$valuesCount - 1] = $notesValues;
        $googleSheetsValues[$k][$valuesCount] = sha1(serialize($contact));

        // Добавим строку гугл таблицы
        if ($idColumn['values'][0]) {
            $foundRowIndex = array_search([$contact->id], $idColumn['values']);
            if ($foundRowIndex !== false) {
                $googleSheetsValues[$k]['row'] = $foundRowIndex + 2;
            }
        }

        // usleep(500000);
    }

    // Приводим таблицу в нужный вид по стандарту гугл таблиц
    $data = [];
    foreach ($googleSheetsValues as $k => $val) {
        if (isset($val['row']) && is_numeric($val['row'])) {
            $data[$k]['row'] = $val['row'];
            unset($val['row']);
            $data[$k]['newArrayValuesUpdate'][] = $val;
        } else {
            $data[$k]['newArrayValuesAppend'][] = $val;
        }
    }

    try {
        $valueRangesUpdate = [];
        $valueRangesAppend = [];
        foreach ($data as $key => $value) {
            // Если указана строка, то значит обновляем сущности
            if (!empty($value['row']) && !empty($value['newArrayValuesUpdate'])) {
                $valueRangesUpdate[] = new Google_Service_Sheets_ValueRange([
                    'range' => $sheetPropertiesTitle . '!A' . $value['row'],
                    'majorDimension' => 'ROWS',
                    'values' => $value['newArrayValuesUpdate']
                ]);
            }
            // Если нет, то добавим новые записи в таблицу
            if (!empty($value['newArrayValuesAppend'])) {
                $valueRangesAppend[] = $value['newArrayValuesAppend'][0];
            }
        }

        // Обновление сущностей пакетно
        if (!empty($valueRangesUpdate)) {
            try {
                $requestBody = new Google_Service_Sheets_BatchUpdateValuesRequest([
                    "valueInputOption" => "RAW",
                    "data" => $valueRangesUpdate
                ]);
                $service->spreadsheets_values->batchUpdate($spreadsheetId, $requestBody);
            } catch (Google_Service_Exception $exception) {
                $reason = $exception->getErrors()[0]['reason'];
                putContent(json_encode($exception->getErrors(), JSON_UNESCAPED_UNICODE), __LINE__);
            }
        }

        // Добавление сущностей пакетно
        if (!empty($valueRangesAppend)) {
            try {
                $body = new Google_Service_Sheets_ValueRange([
                    'values' => $valueRangesAppend
                ]);
                $options = [
                    'valueInputOption' => 'RAW'
                ];
                // Добавим собранные значения
                $service->spreadsheets_values->append($spreadsheetId, $sheetPropertiesTitle . '!A:A', $body, $options);
            } catch (Google_Service_Exception $exception) {
                $reason = $exception->getErrors()[0]['reason'];
                putContent(json_encode($exception->getErrors(), JSON_UNESCAPED_UNICODE), __LINE__);
                die;
            }
        }
    } catch (Google_Service_Exception $exception) {
        $reason = $exception->getErrors()[0]['reason'];
        putContent(json_encode($exception->getErrors(), JSON_UNESCAPED_UNICODE), __LINE__);
        die;
    }
}

// Поиск дублей по id
// Получим id сущностей в гугл таблице
$idColumn = $service->spreadsheets_values->get($spreadsheetId, $sheetPropertiesTitle . '!A2:A', ['valueRenderOption' => 'UNFORMATTED_VALUE']);
$actualIds = [];

foreach ($idColumn['values'] as $id) {
    if (!empty($id)) {
        $actualIds[] = $id[0];
    }
}

$duplicateIds = [];
if (!empty($actualIds)) {
    foreach (array_count_values($actualIds) as $key => $val) {
        if ($val > 1) $duplicateIds[] = $key;
    }
}

putContent(json_encode($duplicateIds),__LINE__);

// Пакетное удаление дублей
if (!empty($duplicateIds)) {
    if ($idColumn['values'][0]) {
        $requests = [];
        $index = 0;
        foreach ($duplicateIds as $k => $duplicateId) {
            $row = array_keys($idColumn['values'], [$duplicateId]);
            if (!empty($row)) {
                $rowId = $row[0] - $index;
                if ($rowId >= 0) {
                    $requests[] = new Google_Service_Sheets_Request([
                        'deleteDimension' => [
                            'range'          => [
                                'sheetId' => (int)$sheetPropertiesId, // ID листа
                                'dimension' => "ROWS",
                                "startIndex" => $rowId + 1,
                                "endIndex" => $rowId + 2
                            ],
                        ]
                    ]);
                    // putContent($duplicateId . ' ' . json_encode($requests),__LINE__);
                    $index++;
                }
            }
        }

        if (!empty($requests)) {
            $batchUpdateRequest = new Google_Service_Sheets_BatchUpdateSpreadsheetRequest([
                'requests' => $requests
            ]);

            try {
                $response = $service->spreadsheets->batchUpdate($spreadsheetId, $batchUpdateRequest);
            } catch (Google_Service_Exception $exception) {
                $reason = $exception->getErrors()[0]['reason'];
                // putContent(json_encode($exception->getErrors(), JSON_UNESCAPED_UNICODE), __LINE__);
            }
        }
    }
}

putContent('Работает конец ' . date("H:i:s", $nowtime) . ' ', __LINE__);
file_put_contents(__DIR__ . '/contacttime.txt', $nowtime);
