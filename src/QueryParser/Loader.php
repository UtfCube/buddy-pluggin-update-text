<?php declare(strict_types=1);

/*
 Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 or any later
 version. You should have received a copy of the GPL license along with this
 program; if you did not, you can find it at http://www.gnu.org/
 */

namespace Manticoresearch\Buddy\Plugin\UpdateText\QueryParser;

use Manticoresearch\Buddy\Core\ManticoreSearch\Endpoint as ManticoreEndpoint;
use Manticoresearch\Buddy\Core\ManticoreSearch\RequestFormat;
use Manticoresearch\Buddy\Plugin\UpdateText\Error\ParserLoadError;

class Loader {

	/**
	 * @param string $reqPath
	 * @param ManticoreEndpoint $reqEndpointBundle
	 * @return UpdateQueryParserInterface
	 */
	public static function getUpdateQueryParser(
		string $reqPath,
		ManticoreEndpoint $reqEndpointBundle
	): UpdateQueryParserInterface {
		// Resolve the possible ambiguity with Manticore query format as it may not correspond to request format
		$reqFormat = match ($reqEndpointBundle) {
			ManticoreEndpoint::Cli, ManticoreEndpoint::CliJson, ManticoreEndpoint::Sql => RequestFormat::SQL,
			'update' => RequestFormat::JSON,
			default => throw new ParserLoadError("Unsupported endpoint bundle '{$reqEndpointBundle->value}' passed"),
		};
		$parserClass = match ($reqFormat) {
			RequestFormat::SQL => 'SQLUpdateParser',
			RequestFormat::JSON => 'JSONUpdateParser',
		};
		$parserClassFull = __NAMESPACE__ . '\\' . $parserClass;
		$parser = new $parserClassFull();
		return $parser;
		// if ($parser instanceof UpdateQueryParserInterface) {
		// 	return $parser;
		// }
	}

}
