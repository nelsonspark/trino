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
package io.trino.plugin.hudi.query;

import com.google.common.base.Joiner;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.FileSlice;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.table.HudiTableFileSystemView;
import io.trino.spi.type.TypeManager;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.util.HudiUtil.buildPartitionKeys;

public class HudiSnapshotQueryDirectoryLister
        extends AbstractDirectoryLister
{
    public HudiSnapshotQueryDirectoryLister(
            HudiTableHandle tableHandle,
            HudiTableFileSystemView fileSystemView,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            TypeManager typeManager)
    {
        super(tableHandle, fileSystemView, hiveMetastore, hiveTable, typeManager);
    }

    public List<FileSlice> listFileSlice(HudiPartitionInfo partitionInfo, String commitTime)
    {
        return fileSystemView.getLatestMergedFileSlicesBeforeOrOn(
                        partitionInfo.getRelativePartitionPath(),
                        commitTime)
                .collect(toImmutableList());
    }

    @Override
    public List<String> getPartitionsInner()
    {
        HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(hadoopConfiguration);
        FileSystemBackedTableMetadata fileSystemBackedTableMetadata = new FileSystemBackedTableMetadata(localEngineContext, new SerializableConfiguration(hadoopConfiguration), tableHandle.getBasePath(), false);

        try {
            return fileSystemBackedTableMetadata.getAllPartitionPaths().stream().map(item -> {
                StringBuilder partitionBuilder = new StringBuilder();
                List<String> values = Arrays.asList(item.split("/"));

                if (partitionColumns.size() != values.size()) {
                    throw new HoodieException("partition columns size don't match partition values");
                }

                for (int i = 0; i < partitionColumns.size(); i++) {
                    partitionBuilder.append(partitionColumns.get(i).getName());
                    partitionBuilder.append("=");
                    partitionBuilder.append(values.get(i));
                    if (i != partitionColumns.size() - 1) {
                        partitionBuilder.append("/");
                    }
                }
                return partitionBuilder.toString();
            }).collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new HudiException("Error get Hudi mor partitions", e);
        }
    }

    public String getRelativePartitionPath(String hivePartitionName)
    {
        return Joiner.on("/").join(Arrays.stream(hivePartitionName.split("/")).map(item -> item.substring(item.indexOf("=") + 1)).collect(Collectors.toList()));
    }

    public List<HivePartitionKey> getHivePartitionKeys(String hivePartitionName)
    {
        List<String> partitionValues = Arrays.stream(hivePartitionName.split("/")).map(item -> item.substring(item.indexOf("=") + 1)).collect(Collectors.toList());
        return buildPartitionKeys(partitionColumns, partitionValues);
    }
}
