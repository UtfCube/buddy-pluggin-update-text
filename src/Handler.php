<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\UpdateText;

use Manticoresearch\Buddy\Core\ManticoreSearch\Client as HTTPClient;
use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithClient;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use Manticoresearch\Buddy\Core\ManticoreSearch\Endpoint as ManticoreEndpoint;
use Manticoresearch\Buddy\Plugin\UpdateText\QueryParser\Datatype;
use RuntimeException;
use parallel\Runtime;
use Exception;
use Manticoresearch\Buddy\Plugin\UpdateText\QueryParser\Datalim;

class Handler extends BaseHandlerWithClient {
	/**
	 * Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

  /**
	 * Process the request
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(Runtime $runtime): Task {
		$taskFn = static function (Payload $handler, HTTPClient $manticoreClient): TaskResult {
			$parsed = $handler->parsed;
			$table = $parsed["UPDATE"][0]["table"];
			$manticoreClient->setPath('sql?mode=raw');
			$set = $parsed["SET"];
			$resp = $manticoreClient->sendRequest("DESC $table");
			if (!isset($resp)) {
				throw new Exception('Empty queries to process');
			}
			$columnsInfo = ((array)json_decode($resp->getBody(), true))[0]['data'];
			$columns = array_map(fn($x) => $x['Field'], $columnsInfo);
			$setFields = array_map(fn($x) => $x['sub_tree'][0]['base_expr'], $set);
			$notExistsColumns = array_values(array_diff($setFields, $columns));
			if (count($notExistsColumns) > 0) {
				throw new Exception("index $table: attribute '$notExistsColumns[0]' not found");
			}
			// TODO: add check valid json
			// TODO: maybe add set fields type check 
			$setFieldsValues = array_map(function ($x) {
				$subTree = $x['sub_tree'];
				return ['field' => $subTree[0]['base_expr'], 'value' => $subTree[2]['base_expr']];
			}, $set);

			$where = $parsed["WHERE"];
			$whereStr = implode(' ', array_map(fn($x) => $x['base_expr'], $where));
			$selectQueryBase = "SELECT * FROM $table WHERE $whereStr";
			$columnsStr = implode(',', $columns);
			
			$limit = 1000;
			$i = 0;
			$total = 0;
			do {
				$offset = $i * $limit;
				$maxMatches = ($i + 1) * $limit;
				$selectQ = "$selectQueryBase ORDER BY id ASC LIMIT $limit OFFSET $offset OPTION max_matches=$maxMatches";
				$resp = $manticoreClient->sendRequest($selectQ);
				if (!isset($resp)) {
					throw new Exception('Empty queries to process');
				}
				$chunk = ((array)json_decode($resp->getBody(), true))[0];
				$rowsChunk = $chunk['data'];
				$rowsCount = count($rowsChunk);
				$total += $rowsCount;
				if ($rowsCount > 0) {
					$rowsStr = array_map(function ($row) use ($setFieldsValues, $columnsInfo) {
						foreach ($setFieldsValues as $newValue) {
							$row[$newValue['field']] = $newValue['value'];
						}

						$escapedRow = array_map(function ($column) use ($row) {
							$value = $row[$column['Field']];
							$value = match ($column['Type']) {
								'text', 'string', 'json' => str_starts_with($value, "'") ? $value : "'$value'",
								'mva', 'mva64' => str_starts_with($value, "(") ? $value : "($value)",
								default => $value,
							};
							return $value;
						}, $columnsInfo);

						$rowStr = implode(',', $escapedRow);
						return "($rowStr)";
					}, $rowsChunk);

					$valuesStr = implode(',', $rowsStr);
					$replaceQ = "REPLACE INTO $table ($columnsStr) VALUES $valuesStr";
					$resp = $manticoreClient->sendRequest($replaceQ);
					if (!isset($resp)) {
						throw new Exception('Empty queries to process');
					}
					print_r(json_decode($resp->getBody(), true));
					if ($rowsCount < $limit) {
						break;
					}
					$i += 1;
				}
				else {
					break;
				}
			} while($i < Datalim::MySqlMaxInt);

			return TaskResult::withTotal($total);
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$this->payload, $this->manticoreClient]
		)->run();
	}
}
