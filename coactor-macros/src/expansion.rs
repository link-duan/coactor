use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemStruct, parse_macro_input};

pub(crate) fn expand_actor(attribute: TokenStream, item: TokenStream) -> TokenStream {
    if !attribute.is_empty() {
        return compile_error(
            "#[actor] takes no attributes; the Actor Type name is passed to register(name) by the consumer",
        );
    }
    let actor_struct = parse_macro_input!(item as ItemStruct);
    let actor_ident = &actor_struct.ident;

    quote! {
        #actor_struct

        impl ::coactor::__macro::ErasedActor for #actor_ident {
            fn activate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
            ) -> ::coactor::__macro::BoxFuture<'a, ::core::result::Result<(), ::std::string::String>> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_ident>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_activate().await.map_err(|error| ::std::format!("{error:?}"))
                })
            }

            fn deactivate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                reason: ::coactor::DeactivationReason,
            ) -> ::coactor::__macro::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_ident>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_deactivate(reason).await
                })
            }

            fn handle<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                ctx: &'a ::coactor::MessageContext,
                payload: &'a [u8],
            ) -> ::coactor::__macro::BoxFuture<'a, ::coactor::__macro::MessageOutcome> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_ident>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_message(ctx, payload).await;
                    ::coactor::__macro::MessageOutcome::Completed
                })
            }

            fn session_opened<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                ctx: &'a ::coactor::MessageContext,
            ) -> ::coactor::__macro::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_ident>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_session_opened(ctx).await
                })
            }

            fn session_closed<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                ctx: &'a ::coactor::MessageContext,
            ) -> ::coactor::__macro::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_ident>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_session_closed(ctx).await
                })
            }
        }

        impl<S> ::coactor::__macro::ActorType<S> for #actor_ident
        where
            #actor_ident: ::coactor::Actor<S>,
        {
            fn create(runtime: ::coactor::__macro::ActorRuntime<S>) -> Self {
                <Self as ::coactor::Actor<S>>::new(runtime)
            }
        }
    }
    .into()
}

pub(crate) fn compile_error(message: &str) -> TokenStream {
    quote!(compile_error!(#message);).into()
}
