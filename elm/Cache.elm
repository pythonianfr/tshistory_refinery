module Cache exposing (main)

import Browser
import Html as H
import Html.Attributes as HA
import Html.Events as HE
import Http
import Json.Decode as D
import Json.Encode as E
import Url.Builder as UB


nocmd model = ( model, Cmd.none )


unwraperror : Http.Error -> String
unwraperror resp =
    case resp of
        Http.BadUrl x -> "bad url: " ++ x
        Http.Timeout -> "the query timed out"
        Http.NetworkError -> "there was a network error"
        Http.BadStatus val -> "we got a bad status answer: " ++ String.fromInt val
        Http.BadBody body -> "we got a bad body: " ++ body


type alias Policy =
    { name : String
    , ready : Bool
    , initial_revdate : String
    , from_date : String
    , look_before : String
    , look_after : String
    , revdate_rule : String
    , schedule_rule : String
    }


type alias Model =
    { baseurl : String
    , policies : List Policy
    , deleting : Maybe String
    , adding : Maybe Policy
    , adderror : String
    }


policydecoder =
    D.map8 Policy
        (D.field "name" D.string)
        (D.field "ready" D.bool)
        (D.field "initial_revdate" D.string)
        (D.field "from_date" D.string)
        (D.field "look_before" D.string)
        (D.field "look_after" D.string)
        (D.field "revdate_rule" D.string)
        (D.field "schedule_rule" D.string)


policiesdecoder =
    D.list policydecoder


getpolicies model =
    Http.get
    { url = UB.crossOrigin model.baseurl
          [ "policies" ] [ ]
    , expect = Http.expectJson GotPolicies policiesdecoder
    }


sendpolicy model policy =
    let policy_encoder =
            [ ("name" , E.string policy.name)
            , ("initial_revdate", E.string policy.initial_revdate)
            , ("from_date", E.string policy.from_date)
            , ("look_before", E.string policy.look_before)
            , ("look_after", E.string policy.look_after)
            , ("revdate_rule", E.string policy.revdate_rule)
            , ("schedule_rule", E.string policy.schedule_rule)
            ]
    in Http.request
    { url = UB.crossOrigin model.baseurl [ "create-policy" ] [ ]
    , method = "PUT"
    , headers = []
    , body = Http.jsonBody <| E.object policy_encoder
    , expect = Http.expectString CreatedPolicy
    , timeout = Nothing
    , tracker = Nothing
    }


deletepolicy model name =
    Http.request
    { url = UB.crossOrigin model.baseurl [ "delete-policy", name ] [ ]
    , method = "DELETE"
    , headers = []
    , body = Http.emptyBody
    , expect = Http.expectString DeletedPolicy
    , timeout = Nothing
    , tracker = Nothing
    }



type Msg
    = GotPolicies (Result Http.Error (List Policy))
    | AskDeletePolicy String
    | CancelDeletePolicy
    | DeletePolicy String
    | DeletedPolicy (Result Http.Error String)
    | NewPolicy
    | PolicyField String String
    | CreatePolicy
    | CreatedPolicy (Result Http.Error String)
    | CancelPolicyCreation


update_policy_field policy fieldname value =
    case fieldname of
        "name" -> { policy | name = value }
        "initial_revdate" -> { policy | initial_revdate = value }
        "from_date" -> { policy | from_date = value }
        "look_before" -> { policy | look_before = value }
        "look_after" -> { policy | look_after = value }
        "revdate_rule" -> { policy | revdate_rule = value }
        "schedule_rule" -> { policy | schedule_rule = value }
        _ -> policy


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        GotPolicies (Ok policies) ->
            ( { model | policies = policies }, Cmd.none )

        GotPolicies (Err err) ->
            nocmd <| model

        -- deletion
        AskDeletePolicy name ->
            ( { model | deleting = Just name }
            , Cmd.none
            )

        CancelDeletePolicy ->
            ( { model | deleting = Nothing }
            , Cmd.none
            )

        DeletePolicy name ->
            ( model, deletepolicy model name )

        DeletedPolicy _ ->
            ( model, getpolicies model )

        -- addition
        NewPolicy ->
            ( { model | adding = Just <| Policy "" False "" "" "" "" "" "" }
            , Cmd.none
            )

        PolicyField field value ->
            case model.adding of
                Nothing -> nocmd model
                Just p ->
                    nocmd { model | adding = Just <| update_policy_field p field value }

        CreatePolicy ->
            case model.adding of
                Nothing -> nocmd model
                Just policy ->
                    ( model, sendpolicy model policy )

        CreatedPolicy (Ok _) ->
            ( { model | adderror = "", adding = Nothing }
            , getpolicies model
            )

        CreatedPolicy (Err err) ->
            let emsg = unwraperror err in
            nocmd { model | adderror = emsg }

        CancelPolicyCreation ->
            nocmd { model | adderror = "", adding = Nothing }


viewdeletepolicyaction model policy =
    let askdelete =
            [ H.button [ HA.class "btn btn-outline-danger"
                       , HA.type_ "button"
                       , HE.onClick (AskDeletePolicy policy.name)
                       ]
                  [ H.text "delete" ]
            ]
    in case model.deleting of
           Nothing -> askdelete
           Just name ->
               if name == policy.name then
                   [ H.button [ HA.class "btn btn-success"
                              , HA.type_ "button"
                              , HE.onClick (DeletePolicy name)
                              ]
                         [ H.text "confirm" ]
                   , H.button [ HA.class "btn btn-warning"
                              , HA.type_ "button"
                              , HE.onClick CancelDeletePolicy
                              ]
                       [ H.text "cancel" ]
                   ]
                   else askdelete


viewpolicy model policy =
    H.li [] (
        [ H.p [] [ H.text <| "name → " ++ policy.name ]
        , H.p [] [ H.text <| "ready → " ++ if policy.ready then "true" else "false" ]
        , H.p [] [ H.text <| "initial rev date → " ++ policy.initial_revdate ]
        , H.p [] [ H.text <| "from date → " ++ policy.from_date ]
        , H.p [] [ H.text <| "look before → " ++ policy.look_before ]
        , H.p [] [ H.text <| "look after → " ++ policy.look_after ]
        , H.p [] [ H.text <| "rev date rule → " ++ policy.revdate_rule ]
        , H.p [] [ H.text <| "schedule rule → " ++ policy.schedule_rule ]
        ] ++ (viewdeletepolicyaction model policy))


newpolicy model =
    let inputs =
            [ ("name", "name", "policy name" )
            , ("initial_revdate", "initial revision date", "e.g. (date \"2022-1-1\")" )
            , ("from_date", "from date", "e.g. (date \"2022-1-1\")" )
            , ("look_before", "look before", "e.g. (shifted (today) #:days -15)" )
            , ("look_after", "look after", "e.g. (shifted (today) #:days 15)" )
            , ("revdate_rule", "revision date rule", "in crontab format" )
            , ("schedule_rule", "schedule rule", "in crontab format" )
            ]

        makeinput (fieldname, displayname, placeholder) =
            [ H.input
                [ HA.class "form-control"
                , HA.placeholder placeholder
                , HE.onInput  (PolicyField fieldname)
                ] []
            , H.label
                [ HA.class "form-check-label"
                , HA.for fieldname]
                [ H.text displayname ]
            ]
    in
    H.div [] <| ( List.concat <| List.map makeinput inputs ) ++
        (
         [ H.button [ HA.class "btn btn-success"
                    , HA.type_ "button"
                    , HE.onClick CreatePolicy
                    ]
               [ H.text "create" ]
         , H.button [ HA.class "btn btn-warning"
                    , HA.type_ "button"
                    , HE.onClick CancelPolicyCreation
                    ]
               [ H.text "cancel" ]
         , H.p [] [ H.text model.adderror ]
         ]
        )


viewpolicies model =
    case model.adding of
        Nothing ->
            H.div []
                [ H.button [ HA.class "btn btn-primary"
                              , HA.type_ "button"
                              , HE.onClick NewPolicy
                              ]
                       [ H.text "create a cache policy" ]
                 , H.ul [] <| List.map (viewpolicy model) model.policies
                ]
        Just policy ->
            newpolicy model


view : Model -> H.Html Msg
view model =
    H.div []
        [ H.h1 [] [ H.text "Policies" ]
        , viewpolicies model
        ]


sub model = Sub.none


type alias Input =
    { baseurl : String }

main : Program Input Model Msg
main =
    let
        init input =
            let model = Model input.baseurl [] Nothing Nothing "" in
            ( model
            , getpolicies model
            )
    in
        Browser.element
            { init = init
            , view = view
            , update = update
            , subscriptions = sub
            }