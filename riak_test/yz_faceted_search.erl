%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%--------------------------------------------------------------------
-module(yz_faceted_search).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

-define(INDEX, <<"restaurants_index">>).
-define(TYPE, <<"restaurants">>).
-define(BUCKET, {?TYPE, <<"goodburger">>}).
-define(SCHEMANAME, <<"restaurants_schema">>).

-define(FACETED_SCHEMA,
<<"<schema name=\"test\" version=\"1.5\">
<fields>
   <field name=\"_yz_id\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" required=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_ed\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_pn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_fpn\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_vtag\" type=\"_yz_str\" indexed=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rt\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rk\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_rb\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>
   <field name=\"_yz_err\" type=\"_yz_str\" indexed=\"true\" stored=\"true\" multiValued=\"false\"/>

   <field name=\"name\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"city\" type=\"string\" indexed=\"true\" stored=\"true\"/>
   <field name=\"state\" type=\"string\" indexed=\"true\" stored=\"true\"/>
</fields>
<uniqueKey>_yz_id</uniqueKey>
<types>
   <fieldType name=\"_yz_str\" class=\"solr.StrField\" sortMissingLast=\"true\" />

   <fieldType name=\"string\" class=\"solr.StrField\" sortMissingLast=\"true\" />
</types>
</schema>">>).

-define(CONFIG,
        [{riak_core,
          [{ring_creation_size, 8}]},
         {yokozuna,
          [{enabled, true}]}
        ]).

confirm() ->
    [Node1|_] = Cluster = rt:build_cluster(4, ?CONFIG),
    rt:wait_for_cluster_service(Cluster, yokozuna),

    ok = yz_rt:create_indexed_bucket_type(Node1, ?TYPE, ?INDEX,
                                          ?SCHEMANAME, ?FACETED_SCHEMA),
    yokozuna_rt:commit(Cluster, ?INDEX),

    put_restaurants(Cluster, ?BUCKET),
    verify_faceted_search(Cluster, ?INDEX),
    pass.

-define(RESTAURANTS,
        [
         {<<"Senate">>, <<"Cincinnati">>, <<"Ohio">>},
         {<<"Boca">>, <<"Cincinnati">>, <<"Ohio">>},
         {<<"Terry's Turf Club">>, <<"Cincinnati">>, <<"Ohio">>},
         {<<"Thurman Cafe">>, <<"Columbus">>, <<"Ohio">>},
         {<<"Otto's">>, <<"Covington">>, <<"Kentucky">>}
        ]).

put_restaurants(Cluster, Bucket) ->
    Restaurants = [create_restaurant_json(Name, City, State) ||
                   {Name, City, State} <- ?RESTAURANTS],
    Keys = yokozuna_rt:gen_keys(length(Restaurants)),
    Pid = rt:pbc(hd(Cluster)),
    lists:foreach(fun({Key, Restaurant}) ->
                          put_restaurant(Pid, Bucket, Key, Restaurant)
                  end,
                  lists:zip(Keys, Restaurants)),
    yokozuna_rt:commit(Cluster, ?INDEX).

-spec create_restaurant_json(binary(), binary(), binary()) -> binary().
create_restaurant_json(Name, City, State) ->
    <<"{\"name\":\"", Name/binary, "\",",
      "\"city\":\"", City/binary, "\",",
      "\"state\":\"", State/binary, "\"}">>.

put_restaurant(Pid, Bucket, Key, Restaurant) ->
    Obj = riakc_obj:new(Bucket, Key, Restaurant, "application/json"),
    riakc_pb_socket:put(Pid, Obj).

verify_faceted_search(Cluster, Index) ->
    HP = hd(yz_rt:host_entries(rt:connection_info(Cluster))),
    lager:info("Query: ~p, ~p, ~p", [HP, Index]),
    {ok, "200", _Hdr, Res} = yz_rt:search(HP, Index, "name", "*",
                                          "facet=true&facet.field=state"),
    Struct = mochijson2:decode(Res),

    NumFound = kvc:path([<<"response">>, <<"numFound">>], Struct),
    ?assertEqual(5, NumFound),

    StateCounts = kvc:path([<<"facet_counts">>,
                            <<"facet_fields">>,
                            <<"state">>],
                           Struct),
    ?assertEqual([<<"Ohio">>,4,<<"Kentucky">>,1], StateCounts).

