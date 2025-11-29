use mlua::prelude::*;

pub fn init_lua() -> LuaResult<Lua> {
    Ok(Lua::new())
}
