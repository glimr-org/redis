-module(glimr_session_test_ffi).
-export([clear_config_cache/0, clear_session_store/0]).

clear_config_cache() ->
    try persistent_term:erase({glimr_config, <<"toml">>}) of
        _ -> nil
    catch
        error:badarg -> nil
    end,
    try persistent_term:erase({glimr_config, <<"cache_stores">>}) of
        _ -> nil
    catch
        error:badarg -> nil
    end.

clear_session_store() ->
    try persistent_term:erase(glimr_session_store) of
        _ -> nil
    catch
        error:badarg -> nil
    end.
