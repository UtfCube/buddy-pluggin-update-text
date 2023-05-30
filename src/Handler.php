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
use RuntimeException;
use parallel\Runtime;
use Exception;

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
		// print_r($this->payload->parsed);
		// $parsed = $this->payload->parsed;
		// $table = $parsed["UPDATE"][0]["table"];
		// $where = $parsed["WHERE"];
		// $whereStr = implode(' ', array_map(fn($x) => $x['base_expr'], $where));
		// $repls = ['%NAME%' => $table, '%WHERE_EXPR%' => $whereStr];
		// $selectQ = strtr('SELECT * FROM %NAME% WHERE %WHERE_EXPR%', $repls);

		// print_r($resp);

		// TODO: your logic goes into closure and should return TaskResult as response
		$taskFn = static function (Payload $handler, HTTPClient $manticoreClient): TaskResult {
			$parsed = $handler->parsed;
			$table = $parsed["UPDATE"][0]["table"];
			$where = $parsed["WHERE"];
			$whereStr = implode(' ', array_map(fn($x) => $x['base_expr'], $where));
			$repls = ['%NAME%' => $table, '%WHERE_EXPR%' => $whereStr];
			$selectQ = strtr('SELECT * FROM %NAME% WHERE %WHERE_EXPR%', $repls);
			$manticoreClient->setPath('sql?mode=raw');

			$resp = $manticoreClient->sendRequest($selectQ);
			if (!isset($resp)) {
				throw new Exception('Empty queries to process');
			}
			print_r(json_decode($resp->getBody(), true));
			return TaskResult::raw((array)json_decode($resp->getBody(), true));
			// return TaskResult::withRow([
			// 	'updatetext' => 'updatetext',
			// ])->column('updatetext', Column::String);
		};

		return Task::createInRuntime(
			$runtime, $taskFn, [$this->payload, $this->manticoreClient]
		)->run();
	}
}
