use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(Serialize)]
pub fn derive_serialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let body = match input.data {
        Data::Struct(s) => {
            let field_names = s.fields.iter().map(|f| &f.ident);
            quote! {
                let mut total = 0;
                #( total += self.#field_names.serialize(&mut w).await?; )*
                Ok(total)
            }
        }
        Data::Enum(e) => {
            let arms = e.variants.iter().enumerate().map(|(i, variant)| {
                let variant_ident = &variant.ident;
                let tag = i as u16;

                match &variant.fields {
                    Fields::Unit => quote! {
                        Self::#variant_ident => {
                            #tag.serialize(&mut w).await?;
                            Ok(1)
                        }
                    },
                    Fields::Unnamed(fields) => {
                        let field_indices = (0..fields.unnamed.len()).map(|j| {
                            quote::format_ident!("f{}", j)
                        });
                        let field_indices_copy = field_indices.clone();
                        quote! {
                            Self::#variant_ident(#(#field_indices),*) => {
                                let mut total = #tag.serialize(&mut w).await?;
                                #( total += #field_indices_copy.serialize(&mut w).await?; )*
                                Ok(total)
                            }
                        }
                    },
                    Fields::Named(fields) => {
                        let field_names = fields.named.iter().map(|f| &f.ident);
                        let field_names_copy = field_names.clone();
                        quote! {
                            Self::#variant_ident { #(#field_names),* } => {
                                let mut total = #tag.serialize(&mut w).await?;
                                #( total += #field_names_copy.serialize(&mut w).await?; )*
                                Ok(total)
                            }
                        }
                    }
                }
            });

            quote! {
                match self { #(#arms)* }
            }
        }
        _ => panic!("Only structs and enums supported"),
    };

    TokenStream::from(quote! {
        impl #impl_generics Serialize for #name #ty_generics #where_clause {
            async fn serialize<W: tokio::io::AsyncWrite + Unpin + Send>(&self, mut w: W) -> std::io::Result<usize> {
                #body
            }
        }
    })
}

#[proc_macro_derive(Deserialize)]
pub fn derive_deserialize(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let body = match input.data {
        Data::Struct(s) => {
            let field_names = s.fields.iter().map(|f| &f.ident);
            let field_types = s.fields.iter().map(|f| &f.ty);
            quote! {
                Ok(Self {
                    // Added .await
                    #( #field_names: <#field_types>::deserialize(&mut r).await?, )*
                })
            }
        }
        Data::Enum(e) => {
            let arms = e.variants.iter().enumerate().map(|(i, variant)| {
                let variant_ident = &variant.ident;
                let index = i as u16;

                match &variant.fields {
                    Fields::Unit => quote! { #index => Ok(Self::#variant_ident), },
                    Fields::Unnamed(fields) => {
                        let field_types = fields.unnamed.iter().map(|f| &f.ty);
                        quote! {
                            #index => Ok(Self::#variant_ident(
                                #( <#field_types>::deserialize(&mut r).await? ),*
                            )),
                        }
                    }
                    Fields::Named(fields) => {
                        let mut v = vec![];
                        for field in fields.named.iter() {
                            let ty = field.ty.clone();
                            let name = field.ident.clone().unwrap();
                            v.push(
                                quote!(
                                    #name : #ty::deserialize(&mut r).await?
                                )
                            );
                        }
                        quote!(
                            #index => Ok(Self::#variant_ident {
                                #(#v),*
                            }),
                        )
                    }
                }
            });

            quote! {
                let tag = u16::deserialize(&mut r).await?;
                match tag {
                    #(#arms)*
                    _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Unknown variant tag")),
                }
            }
        }
        _ => unimplemented!(),
    };

    TokenStream::from(quote! {
        impl #impl_generics Deserialize for #name #ty_generics #where_clause {
            async fn deserialize<R: tokio::io::AsyncRead + Unpin + Send>(mut r: R) -> std::io::Result<Self> {
                #body
            }
        }
    })
}