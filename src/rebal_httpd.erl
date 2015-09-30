% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
% http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(rebal_httpd).

-export([handle_replicate_req/2, handle_view_req/3]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

handle_replicate_req(#httpd{method='POST'} = Req, #db{name=DbName}=_Db) ->
    {Props} = chttpd:json_body_obj(Req),
    Target = binary_to_atom(couch_util:get_value(<<"target">>, Props)),
    Range = couch_util:get_value(<<"range">>, Props),
    Suffix = mem3:shard_suffix(DbName),
    ShardName = list_to_binary("shards/" ++ binary_to_list(Range) ++ "/" ++ binary_to_list(DbName) ++ Suffix),
    SourceShard = #shard{name=ShardName, node=node()},
    TargetShard = #shard{name=ShardName, node=Target},
    Opts = [{batch_size, 1000}, {batch_count, all}, {create_target, true}],
    Resp = mem3_rep:go(SourceShard, TargetShard, Opts),
    case Resp of
        {ok, P} ->
            chttpd:send_json(Req, 200, {[{<<"ok">>, P}]});
        Else ->
            chttpd:send_json(Req, 500, {[{<<"error">>, <<"sneks">>}, {<<"reason">>, Else}]})
    end.

handle_view_req(#httpd{method='POST',
        path_parts=[_, _, _, _, ViewName, <<"_build_shard">>]}=Req, #db{name=DbName}=Db, #doc{id=DDocId}=DDoc) ->
    {Props} = chttpd:json_body_obj(Req),
    Range = couch_util:get_value(<<"range">>, Props),
    Suffix = mem3:shard_suffix(DbName),
    ShardName = list_to_binary("shards/" ++ binary_to_list(Range) ++ "/" ++ binary_to_list(DbName) ++ Suffix),
    {ok, Shard} = couch_db:open_int(ShardName, []),
    {ok, Result} = couch_mrview:query_view(Shard, DDoc, ViewName, []),
    [{meta, [{total, Total}, {offset, Offset}]} | _] = Result,
    chttpd:send_json(Req, 200, {[{<<"ok">>, {[{<<"total">>, Total}, {<<"offset">>, Offset}]}}]});
handle_view_req(Req, Db, DDoc) ->
    chttpd_view:handle_view_req(Req, Db, DDoc).


binary_to_atom(Binary) when is_binary(Binary) ->
    list_to_atom(binary_to_list(Binary)).
