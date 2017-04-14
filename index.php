<?php
/**
 * Created by PhpStorm.
 * User: patrick
 * Date: 14/04/2017
 * Time: 10:05
 */

$ini = parse_ini_file("config.ini");

// ----------------------------------------------------------------
$mysqli = new mysqli($ini['host'], $ini['user'], $ini['pass'], $ini['dbname']);
$sql = array();
$tables = array_flip($ini['table']);

// ----------------------------------------------------------------
function createColumns($mysqli, $tableName, &$tableSyntax, $sql)
{
    $tableTypes = array(1 => 'tinyint',2 => 'smallint',3 => 'int',4 => 'float',5 => 'double',7 => 'timestamp',8 => 'bigint',9 => 'mediumint',10 => 'date',11 => 'time',12 => 'datetime',13 => 'year',16 => 'bit',252 => 'text',253 => 'varchar',254 => 'char',246 => 'decimal');

    $query = "SELECT * FROM ".$tableName." LIMIT 1";
    if ($result = mysqli_query($mysqli, $query)) {
        $tableInfo = mysqli_fetch_fields($result);
        $tableSyntax = "(";
        foreach ($tableInfo as $_tableInfo) {

            $tableSyntax.= $_tableInfo->name.",";

            if(in_array($_tableInfo->type,array(1,2,3,4,5,6,7,253,254,246))) {
                $sql[$_tableInfo->name] = "
                    ALTER TABLE __sync 
                    ADD " . $_tableInfo->name . " " . $tableTypes[$_tableInfo->type] . "(" . $_tableInfo->length . ")
                ";
            }else{
                $sql[$_tableInfo->name] = "
                    ALTER TABLE __sync 
                    ADD " . $_tableInfo->name . " " . $tableTypes[$_tableInfo->type]."
                ";
            }

        }
        $tableSyntax = str_replace(",)",")", $tableSyntax.")");
    }
    return $sql;
}

// ----------------------------------------------------------------
echo "Create column array".PHP_EOL;
foreach ($tables as $_table => &$tableSyntax) {
    $sql = createColumns($mysqli, $_table, $tableSyntax, $sql);
}


// ----------------------------------------------------------------
echo "Create sync table".PHP_EOL;
$mysqli->query("DROP TABLE IF EXISTS __sync");
$mysqli->query("CREATE TABLE __sync (__id INT(11) UNSIGNED AUTO_INCREMENT PRIMARY KEY)");


echo "Merging columns".PHP_EOL;
foreach ($sql as $_sql){
    $mysqli->query($_sql);
}

// ----------------------------------------------------------------
echo "Filling new sync table".PHP_EOL;
foreach ($tables as $_table => &$tableSyntax) {

    echo "Filling new sync table with ".$_table.PHP_EOL;

    if ($result = $mysqli->query("SELECT * FROM ".$_table)) {

        $i = 0;
        while ($obj = $result->fetch_object()) {
            $i++;

            if( ($i % 500) ){
                echo " Batch ".$i." ".$_table.PHP_EOL;
            }


            $sql = "INSERT INTO __sync " . $tableSyntax . " VALUES (";

            foreach ($obj as $key) {
                $sql .= "'".$key . "',";
            }

            $sql .= ")";
            $sql = str_replace(",)", ")", $sql);


            $mysqli->query($sql);
        }

        $result->close();
    }

}

echo "End.".PHP_EOL;
$mysqli->close();