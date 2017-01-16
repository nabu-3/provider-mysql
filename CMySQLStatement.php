<?php

/*  Copyright 2009-2011 Rafael Gutierrez Martinez
 *  Copyright 2012-2013 Welma WEB MKT LABS, S.L.
 *  Copyright 2014-2016 Where Ideas Simply Come True, S.L.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace providers\mysql;

use \nabu\db\CNabuDBAbstractStatement;

/**
 * Class to manage MySQL Statements.
 * @author Rafael Gutierrez <rgutierrez@wiscot.com>
 * @version 3.0.0 Surface
 * @package providers\mysql
 */
final class CMySQLStatement extends CNabuDBAbstractStatement
{
    public function __construct($connector, $resultset)
    {
        parent::__construct($connector, $resultset);

        $this->connector->enqueueStatement($this);
    }

    public function __destruct()
    {
        $this->connector->dequeueStatement($this);
    }

    public function getRowsCount()
    {
        return ($this->statement !== null ? mysqli_num_rows($this->statement) : false);
    }

    public function fetchAsArray()
    {
        return ($this->statement !== null ? mysqli_fetch_array($this->statement) : null);
    }

    public function fetchAsAssoc()
    {
        return ($this->statement !== null ? mysqli_fetch_assoc($this->statement) : null);
    }

    public function fetchAsObject($classname)
    {
        if (is_string($classname)) {
            $object = new $classname();
            if (!($object instanceof \cms\core\interfaces\ICMSDBObject) || !$object->fetch($this)) {
                $object = null;
            }

            return $object;
        }

        return false;
    }

    public function release()
    {
        if ($this->statement !== null && $this->connector !== null) {
            $this->connector->releaseStatement($this->statement);
            $this->statement = null;
            $this->connector->dequeueStatement($this);
        }
    }
}
