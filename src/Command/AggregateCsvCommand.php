<?php

namespace App\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

/**
 * Команда решает задачу https://gist.github.com/pavelkdev/435244a2c2e3a9d8dcb2353511fd9dad
 */
class AggregateCsvCommand extends Command
{
    /**
     * Максимальное количество строк из csv файлов хранимых в памяти
     * Должно быть > 0
     */
    const LIMIT_RECORDS_IN_MEMORY = 400;

    /**
     * Шаблон пути до результирующего файла. 
     * Всегда должен содержать в себе одну подставляемую переменную %s
     */
    const RESULT_FILE_PATH_PATTERN = 'C:/temp/result%s.csv';

    /**
     * Шаблон даты
     */
    const DATE_REGEXP_PATTERN = '/^\d{4}-\d{2}-\d{2}/i';

    /**
     * Имя команды
     */
    protected static $defaultName = 'aggregate-csv';

    protected function configure()
    {
        $this
            ->setDescription('Аггрегирует инфу по всем csv файлам')
            ->addArgument('path', InputArgument::OPTIONAL, 'Путь до папки с csv файлами')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $start = microtime(true);
        $this->init();
        $io = new SymfonyStyle($input, $output);
        $path = $input->getArgument('path');

        if (!$path) {
            $io->error('Вы не указали путь до папки с csv файлами');
        }

        $filesPathes = $this->getAllCsv($path);

        $fileWriteCounter = 0;

        $this->aggregateCsvFiles($filesPathes, static::RESULT_FILE_PATH_PATTERN, $fileWriteCounter);

        $workTime = microtime(true) - $start;

        $io->success(implode(PHP_EOL, [
            'Максимальное количество строк в памяти: ' . static::LIMIT_RECORDS_IN_MEMORY,
            'Количество циклов записи в файл: ' . $fileWriteCounter,
            'Результирующий файл: ' . sprintf(static::RESULT_FILE_PATH_PATTERN, ''),
            'Время работы скрипта: ' . $workTime,
        ]));

        return 0;
    }

    /**
     * Проводит инициализацию команды во время выполнения
     */
    protected function init(): void
    {
        $resultPath = sprintf(static::RESULT_FILE_PATH_PATTERN, '');
        if (file_exists($resultPath)) {
            unlink($resultPath);
        }
    }

    /**
     * Производит агрегацию данных из csv файлов
     *
     * @param array $filesPathes массив путей к csv файлам
     * @param string $resultFilePathPattern шаблон пути до результирующего файла
     * @param int &$fileWriteCounter счётчик количества записей в файл
     */
    protected function aggregateCsvFiles(array $filesPathes, string $resultFilePathPattern = self::RESULT_FILE_PATH_PATTERN, int &$fileWriteCounter = null): void
    {
        $aggregatedData = [];

        foreach ($this->readCsvFilesGenerator($filesPathes) as $data) {
            if (preg_match(static::DATE_REGEXP_PATTERN, $data[0])) {
                foreach ($data as $i => $value) {
                    if (!$i) {
                        continue;
                    }

                    if (!isset($aggregatedData[$data[0]])) {
                        $aggregatedData[$data[0]] = [];
                    }

                    if (!isset($aggregatedData[$data[0]][$i])) {
                        $aggregatedData[$data[0]][$i] = 0;
                    }

                    // Приводим к float, так проще
                    $aggregatedData[$data[0]][$i] += (float) $value;
                }

                if (count($aggregatedData) >= static::LIMIT_RECORDS_IN_MEMORY) {
                    $this->saveAggregatedData($aggregatedData, $resultFilePathPattern);
                    if (is_int($fileWriteCounter)) {
                        $fileWriteCounter++;
                    }
                    $aggregatedData = [];
                }
            }
        }
        if (count($aggregatedData)) {
            $this->saveAggregatedData($aggregatedData, $resultFilePathPattern);
            if (is_int($fileWriteCounter)) {
                $fileWriteCounter++;
            }
            $aggregatedData = [];
        }
    }

    /**
     * Сохраняем аггрегированные данные в результирующий файл
     *
     * Алгоритм:
     * 1. Проверяем, сущетвует ли уже результирующий файл
     * 2. Если файл существует, то построчно суммируем данные из этого файла с данными из $aggregatedData и
     * сохраняем в новый результирующий файл. Удаляем из $aggregatedData уже использованные данные
     * 3. Далее записываем все данные из $aggregatedData в новый результирующий файл, 
     * после чего переходим к шагу 4.
     * 4. Заменяем старый результирующий файл новым.
     *
     * @param array &$aggregatedData ссылка на массив с данными
     * @param string $resultFilePathPattern шаблон пути до результирующего файла
     */
    protected function saveAggregatedData(array $aggregatedData, string $resultFilePathPattern = self::RESULT_FILE_PATH_PATTERN): void
    {
        $resultPath = sprintf($resultFilePathPattern, '');
        $resultPathNew = sprintf($resultFilePathPattern, '_new');
        if ($fd = fopen($resultPathNew, 'w')) {
            fwrite($fd, 'date; A; B; C' . PHP_EOL);
            if (file_exists($resultPath)) {
                foreach ($this->readCsvFilesGenerator([$resultPath]) as $data) {
                    if ($data) {
                        if (preg_match(static::DATE_REGEXP_PATTERN, $data[0])) {
                            if (isset($aggregatedData[$data[0]])) {
                                $data[1] += $aggregatedData[$data[0]][1];
                                $data[2] += $aggregatedData[$data[0]][2];
                                $data[3] += $aggregatedData[$data[0]][3];
                                unset($aggregatedData[$data[0]]);
                            }
                            fwrite($fd, sprintf('%s; %g; %g; %g' . PHP_EOL, ...$data));
                        }
                    }
                }
                unlink($resultPath);
            }

            foreach ($aggregatedData as $date => $data) {
                fwrite($fd, sprintf('%s; %g; %g; %g' . PHP_EOL, $date, ...$data));
            }

            fclose($fd);
            rename($resultPathNew, $resultPath);
        }
    }

    /**
     * Читает csv файлы построчно. Возвращает по одной строке ввиде массива столбцов
     *
     * @param array $filesPathes Пути до csv файлов
     * @return Generator
     */
    protected function readCsvFilesGenerator(array $filesPathes)
    {
        foreach ($filesPathes as $filePath) {
            if ($fd = fopen($filePath, 'r')) {
                while(!feof($fd)) {
                    yield fgetcsv($fd, 0, ';');
                }
                fclose($fd);
            }
        }
    }

    /**
     * Извлекает рекурсивно все csv файлы по переданному пути
     *
     * @param string $path путь до папки с csv файлами
     * @return array массив с путями до csv файлов
     */
    protected function getAllCsv(string $path): array
    {
        $items = scandir($path);

        $csvFiles = [];
        foreach ($items as $item) {
            if ($item === '..' || $item === '.') {
                continue;
            }
            $fullPath = $path . $item;

            if (is_dir($fullPath)) {
                $csvFiles = array_merge($csvFiles, $this->getAllCsv($fullPath . '/'));
                continue;
            }

            if (preg_match('/^.*\.csv$/i', $fullPath)) {
                $csvFiles[] = $fullPath;
                continue;
            }
        }

        return $csvFiles;
    }
}
