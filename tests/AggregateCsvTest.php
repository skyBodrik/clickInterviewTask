<?php

namespace App\Tests;

use PHPUnit\Framework\TestCase;
use App\Command\AggregateCsvCommand as AggregateCsvCommand;

class AggregateCsvTest extends TestCase
{
	public function testSaveAggregatedData()
    {
        $class = new \ReflectionClass(AggregateCsvCommand::class);
        $method = $class->getMethod('saveAggregatedData');
        $method->setAccessible(true);
        $obj = new AggregateCsvCommand();

        $aggregatedData = [];

        $aggregatedData = [
        	'1993-03-21' => [
        		1 => 9,
        		2 => 21,
        		3 => 1,
        	],
        	'1995-03-21' => [
        		1 => 9,
        		2 => 21,
        		3 => 1,
        	],
    	];

    	$resultFile = 'C:/temp/test.csv';

    	unlink($resultFile);

        $method->invoke($obj, $aggregatedData, 'C:/temp/test%s.csv');

        $aggregatedData = [
        	'1995-03-21' => [
        		1 => 0,
        		2 => 10,
        		3 => 2,
        	],
    	];

        $method->invoke($obj, $aggregatedData, 'C:/temp/test%s.csv');

        $expectData = [
        	'date; A; B; C' . PHP_EOL,
        	'1993-03-21; 9; 21; 1' . PHP_EOL,
        	'1995-03-21; 9; 31; 3' . PHP_EOL,
        ];

        if ($fd = fopen($resultFile, 'r')) {
            for ($i = 0; !feof($fd); $i++) {
            	$str = fgets($fd);
            	if ($str) {
                	$this->assertEquals($expectData[$i], $str);
            	}
            }
            fclose($fd);
        }
    }

}
