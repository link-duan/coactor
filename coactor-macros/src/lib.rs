use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, Pat, PathArguments,
    ReturnType, Type, TypePath, parse_macro_input,
};

#[proc_macro_attribute]
pub fn command(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn actor(attribute: TokenStream, item: TokenStream) -> TokenStream {
    if attribute.is_empty() {
        return compile_error(
            "#[actor] requires an explicit stable name: #[actor(name = \"...\")]",
        );
    }
    let actor_name = parse_macro_input!(attribute as ActorAttribute).name;
    let mut actor_impl = parse_macro_input!(item as ItemImpl);

    let actor_type = actor_impl.self_ty.as_ref().clone();
    let actor_ident = match &actor_type {
        Type::Path(path) => path.path.segments.last().unwrap().ident.clone(),
        _ => return compile_error("actor can only be applied to an inherent impl"),
    };
    let ref_ident = format_ident!("{}Ref", actor_ident);
    let constructor = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "new" => Some(method.clone()),
        _ => None,
    });

    let commands: Vec<ImplItemFn> = actor_impl
        .items
        .iter_mut()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => {
                take_command_attribute(&mut method.attrs).then(|| method.clone())
            }
            _ => None,
        })
        .collect();
    let activation = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "on_activate" => Some(method.clone()),
        _ => None,
    });
    let deactivation = actor_impl.items.iter().find_map(|item| match item {
        ImplItem::Fn(method) if method.sig.ident == "on_deactivate" => Some(method.clone()),
        _ => None,
    });

    if commands.is_empty() {
        return compile_error("an actor must declare at least one #[command]");
    }
    let Some(constructor) = constructor else {
        return compile_error(
            "an actor must declare pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    };
    if let Err(message) = validate_constructor(&constructor) {
        return compile_error(message);
    }
    let state_type = match constructor_state_type(&constructor) {
        Ok(state) => state,
        Err(message) => return compile_error(message),
    };
    if let Some(activation) = &activation
        && let Err(message) = validate_activation(activation)
    {
        return compile_error(message);
    }
    if let Some(deactivation) = &deactivation
        && let Err(message) = validate_deactivation(deactivation)
    {
        return compile_error(message);
    }

    let mut generated_messages = Vec::new();
    let mut generated_ref_methods = Vec::new();

    for command in &commands {
        if let Err(message) = validate_command(command) {
            return compile_error(message);
        }

        let method_ident = &command.sig.ident;
        let message_ident = format_ident!("__{}{}Command", actor_ident, pascal_case(method_ident));
        let arguments = command_arguments(command);
        let argument_names: Vec<_> = arguments.iter().map(|(name, _)| name).collect();
        let argument_types: Vec<_> = arguments.iter().map(|(_, ty)| ty).collect();
        let (reply_type, error_type, handler_returns_result) = return_types(&command.sig.output);

        let invoke = if handler_returns_result {
            quote! {
                let outcome = ::coactor::__private::FutureExt::catch_unwind(
                    ::std::panic::AssertUnwindSafe(
                        actor.#method_ident(&context, #(self.#argument_names),*)
                    )
                ).await;
                match outcome {
                    ::core::result::Result::Ok(result) => {
                        let result = result.map_err(::coactor::SendError::HandlerError);
                        let _ = self.reply.send(result);
                        ::coactor::__private::CommandOutcome::Completed
                    }
                    ::core::result::Result::Err(_) => {
                        ::coactor::__private::CommandOutcome::Panicked(
                            ::std::boxed::Box::new(move || {
                                let _ = self.reply.send(::core::result::Result::Err(
                                    ::coactor::SendError::ActorStopped,
                                ));
                            })
                        )
                    }
                }
            }
        } else {
            quote! {
                let outcome = ::coactor::__private::FutureExt::catch_unwind(
                    ::std::panic::AssertUnwindSafe(
                        actor.#method_ident(&context, #(self.#argument_names),*)
                    )
                ).await;
                match outcome {
                    ::core::result::Result::Ok(result) => {
                        let _ = self.reply.send(::core::result::Result::Ok(result));
                        ::coactor::__private::CommandOutcome::Completed
                    }
                    ::core::result::Result::Err(_) => {
                        ::coactor::__private::CommandOutcome::Panicked(
                            ::std::boxed::Box::new(move || {
                                let _ = self.reply.send(::core::result::Result::Err(
                                    ::coactor::SendError::ActorStopped,
                                ));
                            })
                        )
                    }
                }
            }
        };

        generated_messages.push(quote! {
            struct #message_ident {
                #(#argument_names: #argument_types,)*
                reply: ::coactor::__private::tokio::sync::oneshot::Sender<
                    ::core::result::Result<#reply_type, ::coactor::SendError<#error_type>>
                >,
            }

            impl ::coactor::__private::ErasedCommand<#state_type> for #message_ident {
                fn execute<'a>(
                    self: ::std::boxed::Box<Self>,
                    actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                    context: ::coactor::CommandContext,
                ) -> ::coactor::__private::BoxFuture<'a, ::coactor::__private::CommandOutcome> {
                    ::std::boxed::Box::pin(async move {
                        let actor = actor
                            .downcast_mut::<#actor_type>()
                            .expect("CoActor registered an incompatible Actor Type");
                        #invoke
                    })
                }

                fn fail(
                    self: ::std::boxed::Box<Self>,
                    error: ::coactor::__private::RuntimeError,
                ) {
                    let _ = self.reply.send(::core::result::Result::Err(error.into()));
                }
            }
        });

        generated_ref_methods.push(quote! {
            pub async fn #method_ident(
                &self,
                #(#argument_names: #argument_types),*
            ) -> ::core::result::Result<#reply_type, ::coactor::SendError<#error_type>> {
                fn __coactor_assert_send_static<T: ::core::marker::Send + 'static>() {}
                fn __coactor_assert_error<T: ::core::fmt::Debug + ::core::marker::Send + 'static>() {}
                __coactor_assert_send_static::<(#(#argument_types,)*)>();
                __coactor_assert_send_static::<#reply_type>();
                __coactor_assert_error::<#error_type>();
                let (reply, receive) = ::coactor::__private::tokio::sync::oneshot::channel();
                self.inner
                    .send(::std::boxed::Box::new(#message_ident {
                        #(#argument_names,)*
                        reply,
                    }))?;
                receive.await.unwrap_or(::core::result::Result::Err(
                    ::coactor::SendError::ActorStopped,
                ))
            }
        });
    }

    let activate = if activation.is_some() {
        quote! {
            fn activate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
            ) -> ::coactor::__private::BoxFuture<'a, ::core::result::Result<(), ::std::string::String>> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_type>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_activate().await.map_err(|error| ::std::format!("{error:?}"))
                })
            }
        }
    } else {
        quote! {
            fn activate<'a>(
                _actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
            ) -> ::coactor::__private::BoxFuture<'a, ::core::result::Result<(), ::std::string::String>> {
                ::std::boxed::Box::pin(async { ::core::result::Result::Ok(()) })
            }
        }
    };
    let deactivate = if deactivation.is_some() {
        quote! {
            fn deactivate<'a>(
                actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                reason: ::coactor::DeactivationReason,
            ) -> ::coactor::__private::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async move {
                    let actor = actor
                        .downcast_mut::<#actor_type>()
                        .expect("CoActor registered an incompatible Actor Type");
                    actor.on_deactivate(reason).await
                })
            }
        }
    } else {
        quote! {
            fn deactivate<'a>(
                _actor: &'a mut (dyn ::core::any::Any + ::core::marker::Send),
                _reason: ::coactor::DeactivationReason,
            ) -> ::coactor::__private::BoxFuture<'a, ()> {
                ::std::boxed::Box::pin(async {})
            }
        }
    };

    quote! {
        #actor_impl

        #(#generated_messages)*

        #[derive(Clone)]
        pub struct #ref_ident {
            inner: ::coactor::__private::ActorRef<#state_type>,
        }

        impl #ref_ident {
            #(#generated_ref_methods)*
        }

        impl ::coactor::__private::ActorType<#state_type> for #actor_type {
            const NAME: &'static str = #actor_name;
            type Ref = #ref_ident;

            fn create(
                actor_id: ::coactor::ActorId,
                state: ::std::sync::Arc<#state_type>,
            ) -> Self {
                Self::new(actor_id, state)
            }

            #activate
            #deactivate

            fn make_ref(inner: ::coactor::__private::ActorRef<#state_type>) -> Self::Ref {
                #ref_ident { inner }
            }
        }
    }
    .into()
}

struct ActorAttribute {
    name: syn::LitStr,
}

impl syn::parse::Parse for ActorAttribute {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let key: syn::Ident = input.parse()?;
        if key != "name" {
            return Err(syn::Error::new(key.span(), "expected `name = \"...\"`"));
        }
        input.parse::<syn::Token![=]>()?;
        let name: syn::LitStr = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected actor attribute input"));
        }
        Ok(Self { name })
    }
}

fn take_command_attribute(attributes: &mut Vec<Attribute>) -> bool {
    let found = attributes.iter().any(|attribute| {
        attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "command")
    });
    attributes.retain(|attribute| {
        !attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "command")
    });
    found
}

fn validate_command(command: &ImplItemFn) -> Result<(), &'static str> {
    if !matches!(command.vis, syn::Visibility::Public(_)) {
        return Err("#[command] methods must be public");
    }
    if command.sig.asyncness.is_none() {
        return Err("#[command] methods must be async");
    }
    if !has_command_context(command) {
        return Err("#[command] must take &CommandContext after &mut self");
    }
    Ok(())
}

fn validate_constructor(method: &ImplItemFn) -> Result<(), &'static str> {
    if !matches!(method.vis, syn::Visibility::Public(_))
        || method.sig.asyncness.is_some()
        || method.sig.inputs.len() != 2
    {
        return Err(
            "actor constructor must be pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    }
    let Some(FnArg::Typed(argument)) = method.sig.inputs.first() else {
        return Err(
            "actor constructor must be pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    };
    if !type_ends_with(argument.ty.as_ref(), "ActorId")
        || !matches!(
            &method.sig.output,
            ReturnType::Type(_, ty) if matches!(ty.as_ref(), Type::Path(path) if path.path.is_ident("Self"))
        )
    {
        return Err(
            "actor constructor must be pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    }
    Ok(())
}

fn validate_activation(method: &ImplItemFn) -> Result<(), &'static str> {
    if method.sig.asyncness.is_none() {
        return Err("on_activate must be async");
    }
    if method.sig.inputs.len() != 1 {
        return Err("on_activate must take only &mut self");
    }
    if !is_result_of_unit(&method.sig.output) {
        return Err("on_activate must return Result<(), E>");
    }
    Ok(())
}

fn validate_deactivation(method: &ImplItemFn) -> Result<(), &'static str> {
    if method.sig.asyncness.is_none() {
        return Err("on_deactivate must be async");
    }
    if method.sig.inputs.len() != 2 {
        return Err("on_deactivate must take &mut self and DeactivationReason");
    }
    let Some(FnArg::Typed(reason)) = method.sig.inputs.iter().nth(1) else {
        return Err("on_deactivate must take DeactivationReason as its final parameter");
    };
    if !type_ends_with(reason.ty.as_ref(), "DeactivationReason") {
        return Err("on_deactivate must take DeactivationReason as its final parameter");
    }
    if !matches!(method.sig.output, ReturnType::Default) {
        return Err("on_deactivate must not return a value or error");
    }
    Ok(())
}

fn type_ends_with(ty: &Type, name: &str) -> bool {
    matches!(
        ty,
        Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == name)
    )
}

fn is_result_of_unit(output: &ReturnType) -> bool {
    let ReturnType::Type(_, ty) = output else {
        return false;
    };
    let Type::Path(path) = ty.as_ref() else {
        return false;
    };
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    if segment.ident != "Result" {
        return false;
    }
    matches!(
        arguments.args.first(),
        Some(GenericArgument::Type(Type::Tuple(tuple))) if tuple.elems.is_empty()
    )
}

fn constructor_state_type(constructor: &ImplItemFn) -> Result<Type, &'static str> {
    let Some(FnArg::Typed(state)) = constructor.sig.inputs.iter().nth(1) else {
        return Err(
            "actor constructor must be pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self",
        );
    };
    let Type::Path(TypePath { path, .. }) = state.ty.as_ref() else {
        return Err("actor constructor App State must use Arc<AppState>");
    };
    let Some(segment) = path.segments.last() else {
        return Err("actor constructor App State must use Arc<AppState>");
    };
    if segment.ident != "Arc" {
        return Err("actor constructor App State must use Arc<AppState>");
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err("actor constructor App State must use Arc<AppState>");
    };
    match arguments.args.first() {
        Some(GenericArgument::Type(state)) if arguments.args.len() == 1 => Ok(state.clone()),
        _ => Err("actor constructor App State must use Arc<AppState>"),
    }
}

fn has_command_context(command: &ImplItemFn) -> bool {
    let Some(FnArg::Typed(context)) = command.sig.inputs.iter().nth(1) else {
        return false;
    };
    let Type::Reference(reference) = context.ty.as_ref() else {
        return false;
    };
    reference.mutability.is_none() && type_ends_with(reference.elem.as_ref(), "CommandContext")
}

fn command_arguments(command: &ImplItemFn) -> Vec<(syn::Ident, Type)> {
    command
        .sig
        .inputs
        .iter()
        .skip(2)
        .map(|argument| match argument {
            FnArg::Typed(argument) => {
                let Pat::Ident(name) = argument.pat.as_ref() else {
                    panic!("command arguments must use identifier patterns")
                };
                (name.ident.clone(), argument.ty.as_ref().clone())
            }
            FnArg::Receiver(_) => unreachable!(),
        })
        .collect()
}

fn return_types(output: &ReturnType) -> (Type, Type, bool) {
    let output_type = match output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => ty.as_ref().clone(),
    };
    if let Type::Path(path) = &output_type
        && let Some(segment) = path.path.segments.last()
        && segment.ident == "Result"
        && let PathArguments::AngleBracketed(arguments) = &segment.arguments
    {
        let types: Vec<_> = arguments
            .args
            .iter()
            .filter_map(|argument| match argument {
                GenericArgument::Type(ty) => Some(ty.clone()),
                _ => None,
            })
            .collect();
        if types.len() == 2 {
            return (types[0].clone(), types[1].clone(), true);
        }
    }
    (
        output_type,
        syn::parse_quote!(::core::convert::Infallible),
        false,
    )
}

fn pascal_case(ident: &syn::Ident) -> syn::Ident {
    let value = ident.to_string();
    let mut result = String::new();
    let mut uppercase = true;
    for character in value.chars() {
        if character == '_' {
            uppercase = true;
        } else if uppercase {
            result.extend(character.to_uppercase());
            uppercase = false;
        } else {
            result.push(character);
        }
    }
    format_ident!("{result}")
}

fn compile_error(message: &str) -> TokenStream {
    quote!(compile_error!(#message);).into()
}
