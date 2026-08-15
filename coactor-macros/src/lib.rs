use proc_macro::TokenStream;

mod expansion;

#[proc_macro_attribute]
pub fn actor(attribute: TokenStream, item: TokenStream) -> TokenStream {
    expansion::expand_actor(attribute, item)
}
