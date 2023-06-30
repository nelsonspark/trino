/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.partition;

import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_PARTITION_NOT_FOUND;
import static io.trino.plugin.hudi.util.HudiUtil.buildPartitionKeys;
import static io.trino.plugin.hudi.util.HudiUtil.partitionMatchesPredicates;
import static java.lang.String.format;

public class HiveHudiPartitionInfo
        implements HudiPartitionInfo
{
    private final String hivePartitionName;
    private final List<Column> partitionColumns;
    private String relativePartitionPath;
    private List<HivePartitionKey> hivePartitionKeys;

    private final HudiDirectoryLister hudiDirectoryLister;

    public HiveHudiPartitionInfo(
            String hivePartitionName,
            List<Column> partitionColumns,
            HudiDirectoryLister hudiDirectoryLister)
    {
        this.hivePartitionName = hivePartitionName;
        this.partitionColumns = partitionColumns;
        if (partitionColumns.isEmpty()) {
            this.relativePartitionPath = "";
            this.hivePartitionKeys = Collections.emptyList();
        } else {
            this.relativePartitionPath = hudiDirectoryLister.getRelativePartitionPath(hivePartitionName);
            this.hivePartitionKeys = hudiDirectoryLister.getHivePartitionKeys(hivePartitionName);
        }
        this.hudiDirectoryLister = hudiDirectoryLister;
    }

    @Override
    public Table getTable()
    {
        return null;
    }

    @Override
    public String getRelativePartitionPath()
    {
        return relativePartitionPath;
    }

    @Override
    public String getHivePartitionName()
    {
        return hivePartitionName;
    }

    @Override
    public List<HivePartitionKey> getHivePartitionKeys()
    {
        return hivePartitionKeys;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hivePartitionName", hivePartitionName)
                .add("hivePartitionKeys", hivePartitionKeys)
                .toString();
    }
}
