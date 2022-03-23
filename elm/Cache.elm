module Cache exposing (main)

import Browser
import Html as H
import Html.Attributes as HA
import Html.Events as HE
import Http
import Json.Decode as D
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
    | DeletePolicy String
    | DeletedPolicy (Result Http.Error String)


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        GotPolicies (Ok policies) ->
            ( { model | policies = policies }, Cmd.none )

        GotPolicies (Err err) ->
            nocmd <| model

        DeletePolicy name ->
            ( model, deletepolicy model name )

        DeletedPolicy _ ->
            ( model, getpolicies model )


viewpolicy policy =
    H.li []
        [ H.p [] [ H.text <| "name → " ++ policy.name ]
        , H.p [] [ H.text <| "ready → " ++ if policy.ready then "true" else "false" ]
        , H.p [] [ H.text <| "initial rev date → " ++ policy.initial_revdate ]
        , H.p [] [ H.text <| "from date → " ++ policy.from_date ]
        , H.p [] [ H.text <| "look before → " ++ policy.look_before ]
        , H.p [] [ H.text <| "look after → " ++ policy.look_after ]
        , H.p [] [ H.text <| "rev date rule → " ++ policy.revdate_rule ]
        , H.p [] [ H.text <| "schedule rule → " ++ policy.schedule_rule ]
        , H.button [ HA.class "btn btn-outline-danger"
                   , HA.type_ "button"
                   , HE.onClick (DeletePolicy policy.name)
                   ]
            [ H.text "delete" ]
        ]


viewpolicies model =
    H.ul [] <| List.map viewpolicy model.policies


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
            let model = Model input.baseurl [] in
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
