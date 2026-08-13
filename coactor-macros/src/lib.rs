use proc_macro::TokenStream;

mod expansion;
mod syntax;

#[proc_macro_attribute]
pub fn command(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn actor(attribute: TokenStream, item: TokenStream) -> TokenStream {
    expansion::expand_actor(attribute, item)
}
