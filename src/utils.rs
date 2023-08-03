use url::Url;

pub(crate) fn pop_query_param(url: &mut Url, param: String) -> Option<String> {
    let mut pairs: Vec<_> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    let value = pairs
        .iter()
        .position(|(key, _)| key.eq(param.as_str()))
        .map(|idx| pairs.swap_remove(idx).1);

    url.query_pairs_mut()
        .clear()
        .extend_pairs(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));

    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_pop_query_param_existing() {
        let mut url = Url::parse("http://turso.io/?super=yes&sqld=yo").unwrap();
        let param = "sqld".to_string();
        let result = pop_query_param(&mut url, param.clone());
        assert_eq!(result, Some("yo".to_string()));
        assert_eq!(url.query_pairs().count(), 1);
        assert_eq!(url.query_pairs().find(|(key, _)| key == &param), None);
    }

    #[test]
    fn test_pop_query_param_not_existing() {
        let mut url = Url::parse("http://turso.io/?super=yes&sqld=yo").unwrap();
        let param = "ohno".to_string();
        let result = pop_query_param(&mut url, param.clone());
        assert_eq!(result, None);
        assert_eq!(url.as_str(), "http://turso.io/?super=yes&sqld=yo");
    }
}
