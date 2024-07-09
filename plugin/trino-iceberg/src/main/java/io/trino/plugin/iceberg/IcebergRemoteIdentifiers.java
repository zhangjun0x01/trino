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

package io.trino.plugin.iceberg;

import io.trino.plugin.base.mapping.RemoteIdentifiers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.util.Set;
import java.util.stream.Collectors;

public class IcebergRemoteIdentifiers
        implements RemoteIdentifiers
{
    private final RESTSessionCatalog restSessionCatalog;
    private final SessionCatalog.SessionContext sessionContext;

    public IcebergRemoteIdentifiers(RESTSessionCatalog restSessionCatalog, SessionCatalog.SessionContext sessionContext)
    {
        this.restSessionCatalog = restSessionCatalog;
        this.sessionContext = sessionContext;
    }

    @Override
    public Set<String> getRemoteSchemas()
    {
        return restSessionCatalog.listNamespaces(sessionContext).stream().map(Namespace::toString).collect(Collectors.toSet());
    }

    @Override
    public Set<String> getRemoteTables(String remoteSchema)
    {
        return restSessionCatalog.listTables(sessionContext, Namespace.of(remoteSchema)).stream().map(TableIdentifier::name).collect(Collectors.toSet());
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
    {
        return false;
    }
}
