use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, GenericArgument, ImplItemFn, Pat, PathArguments, ReturnType, Type, TypePath,
};

pub(crate) struct ActorAttribute {
    pub(crate) name: syn::LitStr,
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

pub(crate) fn take_command_attribute(attributes: &mut Vec<Attribute>) -> Option<bool> {
    let found = attributes.iter().find_map(|attribute| {
        attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "command")
            .then_some(!matches!(&attribute.meta, syn::Meta::Path(_)))
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

pub(crate) fn validate_command(command: &ImplItemFn, remote: bool) -> Result<(), &'static str> {
    if !matches!(command.vis, syn::Visibility::Public(_)) {
        return Err("#[command] methods must be public");
    }
    if command.sig.asyncness.is_none() {
        return Err("#[command] methods must be async");
    }
    if !has_command_context(command) {
        return Err("#[command] must take &CommandContext after &mut self");
    }
    if remote && command.sig.inputs.len() != 3 {
        return Err(
            "#[command(remote)] must take exactly one Protobuf request after &CommandContext",
        );
    }
    Ok(())
}

pub(crate) fn validate_constructor(method: &ImplItemFn) -> Result<(), &'static str> {
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

pub(crate) fn validate_activation(method: &ImplItemFn) -> Result<(), &'static str> {
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

pub(crate) fn validate_deactivation(method: &ImplItemFn) -> Result<(), &'static str> {
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

pub(crate) fn constructor_state_type(constructor: &ImplItemFn) -> Result<Type, &'static str> {
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

pub(crate) fn command_arguments(command: &ImplItemFn) -> Vec<(syn::Ident, Type)> {
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

pub(crate) fn return_types(output: &ReturnType) -> (Type, Type, bool) {
    let output_type = match output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => ty.as_ref().clone(),
    };
    if let Type::Path(path) = &output_type {
        if let Some(segment) = path.path.segments.last() {
            if segment.ident == "Result" {
                if let PathArguments::AngleBracketed(arguments) = &segment.arguments {
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
            }
        }
    }
    (
        output_type,
        syn::parse_quote!(::core::convert::Infallible),
        false,
    )
}

pub(crate) fn pascal_case(ident: &syn::Ident) -> syn::Ident {
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

pub(crate) fn compile_error(message: &str) -> TokenStream {
    quote!(compile_error!(#message);).into()
}
