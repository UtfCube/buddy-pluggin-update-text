<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/
namespace Manticoresearch\Buddy\Plugin\UpdateText;

use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\ManticoreSearch\Endpoint as ManticoreEndpoint;
use Manticoresearch\Buddy\Core\Network\Request;
use Manticoresearch\Buddy\Core\Plugin\BasePayload;
use Manticoresearch\Buddy\Plugin\UpdateText\QueryParser\Loader;
use PHPSQLParser\PHPSQLParser;

/**
 * This is simple do nothing request that handle empty queries
 * which can be as a result of only comments in it that we strip
 */
final class Payload extends BasePayload {
	public string $path;

	/** @var string $query */
	public array $parsed = [];

  /**
	 * @param Request $request
	 * @return static
	 */
	public static function fromRequest(Request $request): static {
		$self = new static();
		$self->path = $request->path;
		$parser = Loader::getUpdateQueryParser($request->path, $request->endpointBundle);
		$self->parsed = $parser->parse($request->payload);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return bool
	 */
	public static function hasMatch(Request $request): bool {
		$queryLowercase = strtolower($request->payload);

		if ($request->endpointBundle === ManticoreEndpoint::Bulk) {
			return false;
		}

		$isUpdateSQLQuery = match ($request->endpointBundle) {
			ManticoreEndpoint::Sql, ManticoreEndpoint::Cli, ManticoreEndpoint::CliJson => str_starts_with(
				$queryLowercase, 'update'
			),
			default => false,
		};
		// TODO: add support of /update HTTP query
		// $isUpdateHTTPQuery = match ($request->endpointBundle) {
		// 	ManticoreEndpoint::Update => true,
		// 	ManticoreEndpoint::Bulk => str_starts_with($queryLowercase, '"insert"')
		// 	|| str_starts_with($queryLowercase, '"index"'),
		// 	default => false,
		// };
		$isUpdateHTTPQuery = false;

		$isUpdateError = str_contains($request->error, 'attribute ') && str_contains($request->error, ' not found');
		return ($isUpdateError && ($isUpdateSQLQuery || $isUpdateHTTPQuery));
	}
}
